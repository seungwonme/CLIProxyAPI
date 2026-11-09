package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

var fileMagic = []byte("CPATRANSCRIPT\x00\x01\n")

func encodeFrame(frame Frame) ([]byte, error) {
	payload, err := json.Marshal(frame)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 || len(payload) > maxFrameBytes {
		return nil, errors.New("transcript: oversized frame")
	}
	encoded := make([]byte, 4, 4+len(payload))
	binary.BigEndian.PutUint32(encoded[:4], uint32(len(payload)))
	return append(encoded, payload...), nil
}

func decodeFrame(reader io.Reader) (Frame, int64, error) {
	var size [4]byte
	n, err := io.ReadFull(reader, size[:])
	if err != nil {
		return Frame{}, int64(n), err
	}
	length := binary.BigEndian.Uint32(size[:])
	if length == 0 || length > maxFrameBytes {
		return Frame{}, 4, errors.New("transcript: invalid frame length")
	}
	payload := make([]byte, int(length))
	n, err = io.ReadFull(reader, payload)
	if err != nil {
		return Frame{}, int64(4 + n), err
	}
	var frame Frame
	if err = json.Unmarshal(payload, &frame); err != nil {
		return Frame{}, int64(4 + n), errors.New("transcript: invalid frame encoding")
	}
	if frame.Kind == "" || len(frame.Kind) > maxKindBytes {
		return Frame{}, int64(4 + n), errors.New("transcript: invalid frame kind")
	}
	return frame, int64(4 + n), nil
}

// boundedMetadata bounds copying before allocating and stores only a namespaced
// session token. The adapter supplies verified identity and already-masked data.
func boundedMetadata(metadata Metadata) (Metadata, error) {
	if metadata.RequestID == "" || metadata.UserID == "" {
		return Metadata{}, errors.New("transcript: missing identity")
	}
	budget := maxMetadataBytes / 8
	for _, value := range []string{metadata.RequestID, metadata.UserID, metadata.SessionID, metadata.Method, metadata.URL} {
		if len(value) > budget {
			return Metadata{}, errors.New("transcript: oversized metadata")
		}
		budget -= len(value)
	}
	if len(metadata.Headers) > 128 {
		return Metadata{}, errors.New("transcript: too many headers")
	}
	for key, values := range metadata.Headers {
		if len(key)+16 > budget || len(values) > 128 {
			return Metadata{}, errors.New("transcript: oversized headers")
		}
		budget -= len(key) + 16
		for _, value := range values {
			if len(value)+8 > budget {
				return Metadata{}, errors.New("transcript: oversized headers")
			}
			budget -= len(value) + 8
		}
	}
	copyMetadata := metadata
	copyMetadata.RequestID = strings.Clone(metadata.RequestID)
	copyMetadata.UserID = strings.Clone(metadata.UserID)
	copyMetadata.Method = strings.Clone(metadata.Method)
	copyMetadata.URL = strings.Clone(metadata.URL)
	session := metadata.SessionID
	if session == "" {
		copyMetadata.SessionID = namespace("request", metadata.UserID, metadata.RequestID)
	} else {
		copyMetadata.SessionID = namespace("session", metadata.UserID, session)
	}
	copyMetadata.Headers = make(map[string][]string, len(metadata.Headers))
	for key, values := range metadata.Headers {
		copied := make([]string, len(values))
		for index, value := range values {
			copied[index] = strings.Clone(value)
		}
		copyMetadata.Headers[strings.Clone(key)] = copied
	}
	if copyMetadata.Started.IsZero() {
		copyMetadata.Started = time.Now()
	}
	encoded, err := json.Marshal(copyMetadata)
	if err != nil || len(encoded) > maxMetadataBytes {
		return Metadata{}, errors.New("transcript: oversized metadata")
	}
	return copyMetadata, nil
}

// Open accepts only a native request ID (or its native trailing-eight alias).
// Pin publication and deletion exclusion occur under the same index lock.
func (s *Store) Open(requestID string) (*Reader, error) {
	if s == nil || s.cfg.Root == "" {
		return nil, os.ErrNotExist
	}
	s.mu.Lock()
	r := s.latest[requestID]
	if r == nil || r.deleting || r.readable < int64(len(fileMagic)) {
		s.mu.Unlock()
		return nil, os.ErrNotExist
	}
	if s.readers >= s.cfg.MaxActive {
		s.mu.Unlock()
		return nil, errors.New("transcript: reader limit reached")
	}
	s.readers++
	r.pins++
	path, readable := r.path, r.readable
	status := r.status
	if h := r.handle; h != nil {
		status.DroppedChunks, status.DroppedBytes = h.droppedChunks.Load(), h.droppedBytes.Load()
		if h.state.Load()&endFlag != 0 && status.State == Active {
			status.State = EndedIncomplete
			status.Ended = time.Unix(0, h.endedAt.Load())
		}
	}
	s.mu.Unlock()
	root, info, err := openSecureRoot(s.cfg.Root, false)
	if err == nil && !os.SameFile(info, s.rootInfo) {
		_ = root.Close()
		err = errors.New("transcript: root identity changed")
	}
	var file *os.File
	if err == nil {
		file, err = openOwnedFile(root, path)
		if errors.Is(err, os.ErrNotExist) {
			// A concurrent successful seal can change .open to .log after
			// the pin but before publishing r.path. Derive the sealed name
			// from that pinned generated path, never from the request ID.
			if _, name, ok := ownedPath(path); ok && strings.HasSuffix(name, ".open") {
				file, err = openOwnedFile(root, strings.TrimSuffix(path, ".open")+".log")
			}
		}
		_ = root.Close()
	}
	if err != nil {
		s.mu.Lock()
		r.pins--
		s.readers--
		s.mu.Unlock()
		return nil, err
	}
	magic := make([]byte, len(fileMagic))
	_, err = io.ReadFull(file, magic)
	if err == nil && !bytes.Equal(magic, fileMagic) {
		err = errors.New("transcript: invalid file header")
	}
	if err != nil {
		_ = file.Close()
		s.mu.Lock()
		r.pins--
		s.readers--
		s.mu.Unlock()
		return nil, err
	}
	remaining := readable - int64(len(fileMagic))
	// Validate the pinned prefix before callers publish its state in headers.
	// Only one bounded frame is held at a time, and Next still reads the real
	// bytes again so a corrupt tail is not silently converted to clean EOF.
	for left := remaining; left > 0; {
		_, n, errFrame := decodeFrame(io.LimitReader(file, left))
		left -= n
		if errFrame != nil {
			status.State, status.Failure = EndedIncomplete, "read-frame"
			break
		}
	}
	if _, err = file.Seek(int64(len(fileMagic)), io.SeekStart); err != nil {
		_ = file.Close()
		s.mu.Lock()
		r.pins--
		s.readers--
		s.mu.Unlock()
		return nil, err
	}
	return &Reader{store: s, record: r, file: file, remaining: remaining, name: nativeName(r.metadata), status: status}, nil
}

func (r *Reader) Next() (Frame, error) {
	if r == nil {
		return Frame{}, os.ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Frame{}, os.ErrClosed
	}
	if r.remaining == 0 {
		return Frame{}, io.EOF
	}
	frame, n, err := decodeFrame(io.LimitReader(r.file, r.remaining))
	r.remaining -= n
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		r.status.State = EndedIncomplete
		r.status.Failure = "read-frame"
		r.remaining = 0
		return Frame{}, err
	}
	if frame.Kind == "status" {
		// A complete-looking frame in .open is not publication: sync,
		// close or rename may subsequently have failed. The pin snapshot
		// is authoritative for both response headers and rendered status.
		status := r.status
		frame.Status = &status
	}
	return frame, err
}

func (r *Reader) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.file.Close()
	r.store.mu.Lock()
	r.record.pins--
	r.store.readers--
	r.store.mu.Unlock()
	return err
}

func (r *Reader) Status() Status {
	if r == nil {
		return Status{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

func (r *Reader) Name() string {
	if r == nil {
		return ""
	}
	return r.name
}

func safeName(value string, maximum int) string {
	var result strings.Builder
	for _, c := range value {
		if result.Len() >= maximum {
			break
		}
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			result.WriteByte(byte(c))
		} else {
			result.WriteByte('-')
		}
	}
	return strings.Trim(result.String(), "-")
}

func nativeName(metadata Metadata) string {
	path := strings.SplitN(metadata.URL, "?", 2)[0]
	path = safeName(strings.TrimPrefix(path, "/"), 96)
	if path == "" {
		path = "request"
	}
	id := safeName(shortID(metadata.RequestID), 8)
	if id == "" {
		id = "request"
	}
	return fmt.Sprintf("%s-%s-%s.log", path, metadata.Started.Format("2006-01-02T150405"), id)
}
