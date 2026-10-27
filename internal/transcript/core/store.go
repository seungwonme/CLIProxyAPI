package core

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func New(cfg Config) (*Store, error) {
	return newStore(cfg, nil)
}

// newStore permits deterministic operation faults without production fallbacks.
func newStore(cfg Config, hooks *faultHooks) (*Store, error) {
	if cfg.MaxBytes < 0 || cfg.MaxActive < 0 || cfg.QueueBytes < 0 || cfg.MaxRecords < 0 {
		return nil, errors.New("transcript: negative limit")
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = DefaultMaxBytes
	}
	if cfg.MaxActive == 0 {
		cfg.MaxActive = 128
	}
	if cfg.QueueBytes == 0 {
		cfg.QueueBytes = 8 << 20
	}
	if cfg.MaxRecords == 0 {
		cfg.MaxRecords = 4096
	}
	if cfg.MaxActive > 65536 || cfg.MaxRecords > 1<<20 || cfg.QueueBytes > 1<<30 {
		return nil, errors.New("transcript: memory limit too large")
	}
	s := &Store{cfg: cfg, hooks: hooks, records: make(map[string]*record), latest: make(map[string]*record),
		admit: make(chan *Handle, cfg.MaxActive), queue: make(chan capture, queueCapacity(cfg.QueueBytes)),
		wake: make(chan struct{}, 1), done: make(chan struct{}), slots: make(chan struct{}, cfg.MaxActive)}
	if cfg.Root == "" {
		s.closed.Store(true)
		close(s.done)
		return s, nil
	}
	if !filepath.IsAbs(cfg.Root) || filepath.Clean(cfg.Root) == string(filepath.Separator) {
		return nil, errors.New("transcript: absolute dedicated root required")
	}
	s.cfg.Root = filepath.Clean(cfg.Root)
	root, info, err := openSecureRoot(s.cfg.Root, true)
	if err != nil {
		return nil, err
	}
	s.root, s.rootInfo = root, info
	lock, err := acquireRootLock(root)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	lockedInfo, err := lock.Stat()
	if err != nil || !os.SameFile(info, lockedInfo) {
		_ = lock.Close()
		_ = root.Close()
		return nil, errors.New("transcript: root identity changed")
	}
	s.rootLock = lock
	if err = s.recover(); err != nil {
		_ = root.Close()
		_ = lock.Close()
		return nil, err
	}
	s.accepting.Store(true)
	go s.run()
	return s, nil
}

func queueCapacity(bytes int64) int {
	n := bytes / queueOverhead
	if n < 1 {
		n = 1
	}
	if n > 65536 {
		n = 65536
	}
	return int(n)
}

func (s *Store) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Begin never waits for the worker or acquires a worker-owned mutex.
func (s *Store) Begin(metadata Metadata) *Handle {
	if s == nil {
		return nil
	}
	s.beginning.Add(1)
	defer func() { s.beginning.Add(-1); s.notify() }()
	if !s.accepting.Load() {
		s.skipped.Add(1)
		return nil
	}
	select {
	case s.slots <- struct{}{}:
	default:
		s.skipped.Add(1)
		s.degraded.Store(true)
		return nil
	}
	if !s.accepting.Load() {
		<-s.slots
		s.skipped.Add(1)
		return nil
	}
	copyMetadata, err := boundedMetadata(metadata)
	if err != nil {
		<-s.slots
		s.skipped.Add(1)
		s.degraded.Store(true)
		return nil
	}
	h := &Handle{store: s, metadata: copyMetadata}
	s.active.Add(1)
	select {
	case s.admit <- h:
		return h
	default:
		s.active.Add(-1)
		<-s.slots
		s.skipped.Add(1)
		s.degraded.Store(true)
		return nil
	}
}

// acquire makes terminal publication and in-flight capture exclusion atomic.
func (h *Handle) acquire() bool {
	if h.state.Add(1)&endFlag == 0 {
		return true
	}
	h.release()
	return false
}

func (h *Handle) release() {
	h.state.Add(^uint64(0))
	h.store.notify()
}

// Capture copies only admitted bounded chunks. Queue pressure drops the tail;
// no call waits for disk, a queue drain, a finalizer or a worker-owned mutex.
func (h *Handle) Capture(kind string, data []byte) {
	if h == nil || h.store == nil {
		return
	}
	if !h.acquire() {
		h.store.loss(1, uint64(len(data)))
		return
	}
	defer h.release()
	if len(kind) == 0 || len(kind) > maxKindBytes || kind == "metadata" || kind == "gap" || kind == "status" {
		h.loss(1, uint64(len(data)))
		return
	}
	s := h.store
	at := time.Now()
	for offset := 0; ; {
		end := offset + maxChunkBytes
		if end > len(data) {
			end = len(data)
		}
		charge := int64(end-offset+len(kind)) + queueOverhead
		if !s.reserveQueue(charge) {
			h.loss(1, uint64(len(data)-offset))
			return
		}
		c := capture{handle: h, kind: strings.Clone(kind), at: at, data: append([]byte(nil), data[offset:end]...), charge: charge}
		h.pending.Add(1)
		select {
		case s.queue <- c:
			s.notify()
		default:
			h.pending.Add(-1)
			s.queuedBytes.Add(-charge)
			h.loss(1, uint64(len(data)-offset))
			return
		}
		if end == len(data) {
			return
		}
		offset = end
	}
}

func (s *Store) reserveQueue(bytes int64) bool {
	for attempt := 0; attempt < 16; attempt++ {
		old := s.queuedBytes.Load()
		if bytes > s.cfg.QueueBytes-old {
			return false
		}
		if s.queuedBytes.CompareAndSwap(old, old+bytes) {
			return true
		}
	}
	return false
}

func (s *Store) loss(chunks, bytes uint64) {
	s.droppedChunks.Add(chunks)
	s.droppedBytes.Add(bytes)
	if chunks != 0 || bytes != 0 {
		s.degraded.Store(true)
	}
}

func (h *Handle) loss(chunks, bytes uint64) {
	h.droppedChunks.Add(chunks)
	h.droppedBytes.Add(bytes)
	h.store.loss(chunks, bytes)
}

func (h *Handle) Drop(chunks, bytes uint64) {
	if h == nil || h.store == nil {
		return
	}
	if !h.acquire() {
		h.store.loss(chunks, bytes)
		return
	}
	h.loss(chunks, bytes)
	h.release()
}

// End publishes independently of both bounded queues. The first End wins.
func (h *Handle) End(complete bool) {
	if h == nil || h.store == nil {
		return
	}
	if !h.endedAt.CompareAndSwap(0, time.Now().UnixNano()) {
		return
	}
	bits := endFlag
	if complete {
		bits |= completeFlag
	}
	h.state.Or(bits)
	h.store.notify()
}

// Stop is nonblocking. Active requests remain active until their actual End.
func (s *Store) Stop() {
	if s == nil {
		return
	}
	s.accepting.Store(false)
	s.stopping.Store(true)
	s.notify()
}

// Close is an administrative barrier. Callers must end admitted requests first.
// Reads remain available afterward through identity-checked temporary roots.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.Stop()
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

func (s *Store) Stats() Stats {
	if s == nil {
		return Stats{}
	}
	s.mu.Lock()
	result := Stats{LogicalBytes: s.logical, ReservedBytes: s.reserved, LimitBytes: s.cfg.MaxBytes, Records: len(s.records)}
	s.mu.Unlock()
	result.Active = int(s.active.Load())
	result.DroppedChunks, result.DroppedBytes = s.droppedChunks.Load(), s.droppedBytes.Load()
	result.Skipped, result.Failures, result.Degraded = s.skipped.Load(), s.failures.Load(), s.degraded.Load()
	return result
}

func (s *Store) run() {
	handles := make(map[*Handle]*record, s.cfg.MaxActive)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	defer close(s.done)
	for {
		if s.hooks != nil && s.hooks.beforeWorker != nil {
			s.hooks.beforeWorker()
		}
		for count := 0; count < s.cfg.MaxActive; count++ {
			select {
			case h := <-s.admit:
				handles[h] = s.startRecord(h)
			default:
				count = s.cfg.MaxActive
			}
		}
		for count := 0; count < 64; count++ {
			select {
			case c := <-s.queue:
				r, ok := handles[c.handle]
				if !ok {
					// Capture can race admission consumption, but its admission
					// was published before its handle became visible to callers.
					for !ok {
						h := <-s.admit
						handles[h] = s.startRecord(h)
						r, ok = handles[c.handle]
					}
				}
				s.consume(r, c)
				c.handle.pending.Add(-1)
				s.queuedBytes.Add(-c.charge)
			default:
				count = 64
			}
		}
		for h, r := range handles {
			state := h.state.Load()
			if state&endFlag == 0 {
				continue
			}
			s.publishEnd(r, h)
			if state&captureMask != 0 || h.pending.Load() != 0 {
				continue
			}
			s.finalize(r, h, state&completeFlag != 0)
			delete(handles, h)
			s.active.Add(-1)
			<-s.slots
		}
		if s.stopping.Load() && s.beginning.Load() == 0 && s.active.Load() == 0 {
			if err := s.root.Close(); err != nil {
				s.failure(nil, "root-close")
			}
			if err := s.rootLock.Close(); err != nil {
				s.failure(nil, "root-lock-close")
			}
			s.closed.Store(true)
			return
		}
		if len(s.admit) != 0 || len(s.queue) != 0 {
			continue
		}
		select {
		case <-s.wake:
		case <-ticker.C:
		}
	}
}

func (s *Store) startRecord(h *Handle) *record {
	r := &record{handle: h, metadata: h.metadata, status: Status{State: Active}}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		s.failure(r, "random")
		return r
	}
	r.id = hex.EncodeToString(random[:])
	r.dir = namespace("user", h.metadata.UserID, "")
	r.path = r.dir + "/" + r.id + ".open"
	metadata, err := encodeFrame(Frame{Kind: "metadata", At: h.metadata.Started, Metadata: &h.metadata})
	if err != nil {
		s.failure(r, "metadata")
		return r
	}
	if !s.reserve(r, int64(len(fileMagic)+len(metadata))+terminalReserve, true) {
		s.skipped.Add(1)
		s.failure(r, "capacity")
		return r
	}
	dir, err := ownedDirectory(s.root, r.dir, true)
	if err != nil {
		s.releaseReservation(r)
		s.failure(r, "directory")
		return r
	}
	r.dirInfo, err = dir.Stat(".")
	if err != nil {
		_ = dir.Close()
		s.releaseReservation(r)
		s.failure(r, "directory")
		return r
	}
	file, err := dir.OpenFile(r.id+".open", os.O_WRONLY|os.O_CREATE|os.O_EXCL|openNoFollow, 0600)
	if err == nil {
		r.fileInfo, err = file.Stat()
	}
	_ = dir.Close()
	if err != nil {
		if file != nil {
			_ = file.Close()
		}
		s.releaseReservation(r)
		s.failure(r, "create")
		return r
	}
	r.file = file
	s.mu.Lock()
	s.records[r.id] = r
	s.indexLocked(r)
	s.mu.Unlock()
	if err = s.writeReserved(r, fileMagic); err == nil {
		s.mu.Lock()
		r.readable = int64(len(fileMagic))
		s.mu.Unlock()
		err = s.appendReserved(r, metadata)
	}
	if err != nil {
		s.failure(r, "write")
	}
	return r
}

func (s *Store) consume(r *record, c capture) {
	if r.file == nil || r.failed {
		c.handle.loss(1, uint64(len(c.data)))
		return
	}
	s.writeGap(r)
	if r.failed {
		c.handle.loss(1, uint64(len(c.data)))
		return
	}
	encoded, err := encodeFrame(Frame{Kind: c.kind, At: c.at, Data: c.data})
	if err != nil {
		c.handle.loss(1, uint64(len(c.data)))
		s.failure(r, "encode")
		return
	}
	if !s.reserve(r, int64(len(encoded)), false) {
		c.handle.loss(1, uint64(len(c.data)))
		return
	}
	if err = s.appendReserved(r, encoded); err != nil {
		c.handle.loss(1, uint64(len(c.data)))
		s.failure(r, "write")
	}
}

func (s *Store) writeGap(r *record) {
	h := r.handle
	chunks, bytes := h.droppedChunks.Load(), h.droppedBytes.Load()
	if chunks == r.reportedChunks && bytes == r.reportedBytes {
		return
	}
	status := Status{State: Active, DroppedChunks: chunks, DroppedBytes: bytes}
	encoded, err := encodeFrame(Frame{Kind: "gap", At: time.Now(), Status: &status})
	if err != nil || !s.reserve(r, int64(len(encoded)), false) {
		return
	}
	if err = s.appendReserved(r, encoded); err != nil {
		s.failure(r, "write")
		return
	}
	r.reportedChunks, r.reportedBytes = chunks, bytes
}

func (s *Store) publishEnd(r *record, h *Handle) {
	s.mu.Lock()
	if r.status.State == Active {
		r.status.State = EndedIncomplete
		r.status.Ended = time.Unix(0, h.endedAt.Load())
	}
	s.mu.Unlock()
}

func (s *Store) finalize(r *record, h *Handle, complete bool) {
	s.mu.Lock()
	status := r.status
	s.mu.Unlock()
	status.DroppedChunks, status.DroppedBytes = h.droppedChunks.Load(), h.droppedBytes.Load()
	if complete && !r.failed && status.DroppedChunks == 0 && status.DroppedBytes == 0 {
		status.State = EndedComplete
	}
	if r.file != nil {
		if !r.failed {
			encoded, err := encodeFrame(Frame{Kind: "status", At: status.Ended, Status: &status})
			if err != nil || int64(len(encoded)) > r.reserved {
				s.failure(r, "terminal")
			} else if err = s.appendReserved(r, encoded); err != nil {
				s.failure(r, "write")
			}
		}
		if !r.failed {
			var err error
			if s.hooks != nil && s.hooks.sync != nil {
				err = s.hooks.sync(r.file)
			} else {
				err = r.file.Sync()
			}
			if err != nil {
				s.failure(r, "sync")
			}
		}
		var closeErr error
		if s.hooks != nil && s.hooks.close != nil {
			closeErr = s.hooks.close(r.file)
		} else {
			closeErr = r.file.Close()
		}
		if closeErr != nil {
			_ = r.file.Close()
			s.failure(r, "close")
		}
		r.file = nil
		if !r.failed {
			newPath := strings.TrimSuffix(r.path, ".open") + ".log"
			dir, name, err := s.recordDirectory(r)
			if err == nil {
				newName := strings.TrimSuffix(name, ".open") + ".log"
				_, err = dir.Lstat(newName)
				if os.IsNotExist(err) {
					if s.hooks != nil && s.hooks.rename != nil {
						err = s.hooks.rename(s.root, r.path, newPath)
					} else {
						err = dir.Rename(name, newName)
					}
				} else if err == nil {
					err = errors.New("transcript: seal destination already exists")
				}
				_ = dir.Close()
			}
			if err != nil {
				s.failure(r, "rename")
			} else {
				s.mu.Lock()
				r.path = newPath
				s.mu.Unlock()
			}
		}
	}
	s.releaseReservation(r)
	s.mu.Lock()
	status.Failure = r.status.Failure
	if r.failed {
		status.State = EndedIncomplete
	}
	r.status, r.retired = status, true
	s.mu.Unlock()
}

func (s *Store) failure(r *record, code string) {
	s.failures.Add(1)
	s.degraded.Store(true)
	s.mu.Lock()
	if r != nil {
		r.failed = true
		if r.status.Failure == "" {
			r.status.Failure = code
		}
	}
	if s.closeErr == nil {
		s.closeErr = errors.New("transcript: " + code)
	}
	s.mu.Unlock()
}

// reserve and eviction are worker-only; mu makes their state visible atomically
// to readers and Stats, without keeping a mutex held during filesystem calls.
func (s *Store) reserve(r *record, bytes int64, needsRecord bool) bool {
	for {
		s.mu.Lock()
		fits := bytes <= s.cfg.MaxBytes && s.logical <= s.cfg.MaxBytes-bytes && s.reserved <= s.cfg.MaxBytes-bytes-s.logical
		if fits && (!needsRecord || len(s.records) < s.cfg.MaxRecords) {
			s.reserved += bytes
			r.reserved += bytes
			s.mu.Unlock()
			return true
		}
		s.mu.Unlock()
		if !s.evictOldest() {
			s.degraded.Store(true)
			return false
		}
	}
}

func (s *Store) releaseReservation(r *record) {
	s.mu.Lock()
	s.reserved -= r.reserved
	r.reserved = 0
	s.mu.Unlock()
}

func (s *Store) writeReserved(r *record, data []byte) error {
	var n int
	var err error
	if s.hooks != nil && s.hooks.write != nil {
		n, err = s.hooks.write(r.file, data)
	} else {
		n, err = r.file.Write(data)
	}
	if n < 0 || n > len(data) {
		return errors.New("transcript: invalid write count")
	}
	s.mu.Lock()
	r.bytes += int64(n)
	s.logical += int64(n)
	r.reserved -= int64(len(data))
	s.reserved -= int64(len(data))
	s.mu.Unlock()
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return err
}

func (s *Store) appendReserved(r *record, data []byte) error {
	if err := s.writeReserved(r, data); err != nil {
		return err
	}
	s.mu.Lock()
	r.readable = r.bytes
	s.mu.Unlock()
	return nil
}

func (s *Store) evictOldest() bool {
	s.mu.Lock()
	var oldest *record
	for _, r := range s.records {
		if !r.retired || r.pins != 0 || r.deleting {
			continue
		}
		if oldest == nil || r.status.Ended.Before(oldest.status.Ended) || (r.status.Ended.Equal(oldest.status.Ended) && r.id < oldest.id) {
			oldest = r
		}
	}
	if oldest == nil {
		s.mu.Unlock()
		return false
	}
	oldest.deleting = true
	s.mu.Unlock()
	dir, name, err := s.recordDirectory(oldest)
	if err == nil {
		if s.hooks != nil && s.hooks.unlink != nil {
			err = s.hooks.unlink(s.root, oldest.path)
		} else {
			err = dir.Remove(name)
		}
		_ = dir.Close()
	}
	if err != nil {
		s.mu.Lock()
		oldest.deleting = false
		s.mu.Unlock()
		s.failure(nil, "unlink")
		return false
	}
	s.mu.Lock()
	s.logical -= oldest.bytes
	delete(s.records, oldest.id)
	s.reindexLocked()
	s.mu.Unlock()
	return true
}

func newer(a, b *record) bool {
	if !a.metadata.Started.Equal(b.metadata.Started) {
		return a.metadata.Started.After(b.metadata.Started)
	}
	if !a.status.Ended.Equal(b.status.Ended) {
		return a.status.Ended.After(b.status.Ended)
	}
	return a.id > b.id
}

func shortID(id string) string {
	id = strings.TrimSpace(id)
	if len(id) > 8 {
		return id[len(id)-8:]
	}
	return id
}

func (s *Store) indexLocked(r *record) {
	if r.metadata.RequestID == "" {
		return
	}
	for _, key := range []string{r.metadata.RequestID, shortID(r.metadata.RequestID)} {
		if previous := s.latest[key]; previous == nil || newer(r, previous) {
			s.latest[key] = r
		}
	}
}

func (s *Store) reindexLocked() {
	clear(s.latest)
	for _, r := range s.records {
		if !r.deleting {
			s.indexLocked(r)
		}
	}
}

func namespace(kind, user, session string) string {
	h := sha256.New()
	var size [8]byte
	for _, value := range []string{kind, user, session} {
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = h.Write(size[:])
		_, _ = io.WriteString(h, value)
	}
	return hex.EncodeToString(h.Sum(nil))
}
