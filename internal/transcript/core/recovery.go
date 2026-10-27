package core

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// openSecureRoot walks one component at a time. Lstat and SameFile checks
// supplement directory-relative confinement: symbolic links are never accepted
// as roots, namespace directories or transcript files.
func openSecureRoot(path string, create bool) (*os.Root, os.FileInfo, error) {
	if err := checkCapturePlatform(); err != nil {
		return nil, nil, err
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, nil, errors.New("transcript: unsafe root")
	}
	current, err := os.OpenRoot(string(filepath.Separator))
	if err != nil {
		return nil, nil, err
	}
	parts := strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator))
	var info os.FileInfo
	for _, part := range parts {
		if !safeComponent(part) {
			_ = current.Close()
			return nil, nil, errors.New("transcript: unsafe root component")
		}
		info, err = current.Lstat(part)
		if os.IsNotExist(err) && create {
			err = current.Mkdir(part, 0700)
			if err == nil || os.IsExist(err) {
				info, err = current.Lstat(part)
			}
		}
		if err != nil {
			_ = current.Close()
			return nil, nil, err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			_ = current.Close()
			return nil, nil, errors.New("transcript: symbolic or non-directory root")
		}
		next, errOpen := current.OpenRoot(part)
		if errOpen != nil {
			_ = current.Close()
			return nil, nil, errOpen
		}
		opened, errStat := next.Stat(".")
		_ = current.Close()
		if errStat != nil || !os.SameFile(info, opened) {
			_ = next.Close()
			return nil, nil, errors.New("transcript: root identity changed")
		}
		current = next
	}
	if info == nil {
		_ = current.Close()
		return nil, nil, errors.New("transcript: dedicated root required")
	}
	file, err := current.OpenFile(".", os.O_RDONLY|openNoFollow|openDirectory, 0)
	if err != nil {
		_ = current.Close()
		return nil, nil, err
	}
	if create {
		err = file.Chmod(0700)
	} else if info.Mode().Perm()&0077 != 0 {
		err = errors.New("transcript: unsafe root permissions")
	}
	_ = file.Close()
	if err != nil {
		_ = current.Close()
		return nil, nil, err
	}
	return current, info, nil
}

func safeComponent(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\\x00")
}

func opaque(name string, length int) bool {
	if len(name) != length {
		return false
	}
	for _, c := range name {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

func ownedDirectory(root *os.Root, name string, create bool) (*os.Root, error) {
	if !opaque(name, 64) {
		return nil, errors.New("transcript: unsafe namespace")
	}
	info, err := root.Lstat(name)
	if os.IsNotExist(err) && create {
		err = root.Mkdir(name, 0700)
		if err == nil || os.IsExist(err) {
			info, err = root.Lstat(name)
		}
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("transcript: unsafe namespace directory")
	}
	file, err := root.OpenFile(name, os.O_RDONLY|openNoFollow|openDirectory, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, errors.New("transcript: namespace identity changed")
	}
	dir, err := root.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	opened, err = dir.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		_ = dir.Close()
		return nil, errors.New("transcript: namespace identity changed")
	}
	after, err := root.Lstat(name)
	if err != nil || !after.IsDir() || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, after) {
		_ = dir.Close()
		return nil, errors.New("transcript: namespace changed during open")
	}
	return dir, nil
}

func ownedPath(path string) (string, string, bool) {
	parts := strings.Split(path, "/")
	if len(parts) != 2 || !opaque(parts[0], 64) {
		return "", "", false
	}
	ext := filepath.Ext(parts[1])
	id := strings.TrimSuffix(parts[1], ext)
	if !opaque(id, 32) || ext != ".open" && ext != ".log" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func openOwnedFile(root *os.Root, path string) (*os.File, error) {
	namespace, name, ok := ownedPath(path)
	if !ok {
		return nil, errors.New("transcript: unsafe transcript path")
	}
	dir, err := ownedDirectory(root, namespace, false)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	info, err := dir.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("transcript: unsafe transcript file")
	}
	file, err := dir.OpenFile(name, os.O_RDONLY|openNoFollow|openNonblock, 0)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		_ = file.Close()
		return nil, errors.New("transcript: transcript identity changed")
	}
	after, err := dir.Lstat(name)
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(info, after) {
		_ = file.Close()
		return nil, errors.New("transcript: transcript changed during open")
	}
	return file, nil
}

// recordDirectory checks the identities retained at creation or recovery.
// Mutations use this opened directory, not a freshly resolved namespace path.
func (s *Store) recordDirectory(r *record) (*os.Root, string, error) {
	namespace, name, ok := ownedPath(r.path)
	if !ok || namespace != r.dir || r.dirInfo == nil || r.fileInfo == nil {
		return nil, "", errors.New("transcript: unknown transcript identity")
	}
	dir, err := ownedDirectory(s.root, namespace, false)
	if err != nil {
		return nil, "", err
	}
	info, err := dir.Stat(".")
	if err != nil || !os.SameFile(r.dirInfo, info) {
		_ = dir.Close()
		return nil, "", errors.New("transcript: namespace identity changed")
	}
	info, err = dir.Lstat(name)
	if err != nil {
		_ = dir.Close()
		return nil, "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || !os.SameFile(r.fileInfo, info) {
		_ = dir.Close()
		return nil, "", errors.New("transcript: transcript identity changed")
	}
	return dir, name, nil
}

// recover counts every regular file before admission, including unknown files.
// Unknown files are never indexed for deletion. The bounded index refuses an
// overlarge owned inventory rather than silently losing eviction ownership.
func (s *Store) recover() error {
	if err := s.walkRecovery(s.root, "", 0); err != nil {
		return err
	}
	for s.logical > s.cfg.MaxBytes {
		if !s.evictOldest() {
			return errors.New("transcript: recovered storage exceeds MaxBytes")
		}
	}
	return nil
}

func (s *Store) walkRecovery(root *os.Root, prefix string, depth int) error {
	if depth > 64 {
		return errors.New("transcript: root nesting too deep")
	}
	directory, err := root.OpenFile(".", os.O_RDONLY|openNoFollow|openDirectory, 0)
	if err != nil {
		return err
	}
	defer directory.Close()
	dirInfo, err := directory.Stat()
	if err != nil {
		return err
	}
	for {
		entries, errRead := directory.ReadDir(128)
		for _, entry := range entries {
			name := entry.Name()
			if !safeComponent(name) {
				return errors.New("transcript: unsafe root entry")
			}
			info, err := root.Lstat(name)
			if err != nil {
				return err
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return errors.New("transcript: symbolic root entry")
			}
			path := prefix + name
			if info.IsDir() {
				sub, err := root.OpenRoot(name)
				if err != nil {
					return err
				}
				opened, err := sub.Stat(".")
				if err != nil || !os.SameFile(info, opened) {
					_ = sub.Close()
					return errors.New("transcript: recovery directory changed")
				}
				err = s.walkRecovery(sub, path+"/", depth+1)
				_ = sub.Close()
				if err != nil {
					return err
				}
				continue
			}
			if !info.Mode().IsRegular() {
				return errors.New("transcript: non-regular root entry")
			}
			if info.Size() > int64(^uint64(0)>>1)-s.logical {
				return errors.New("transcript: accounting overflow")
			}
			s.logical += info.Size()
			if _, _, ok := ownedPath(path); !ok {
				s.degraded.Store(true)
				continue
			}
			if len(s.records) >= s.cfg.MaxRecords {
				return errors.New("transcript: recovered inventory exceeds MaxRecords")
			}
			r := s.recoverRecord(path, info)
			r.dirInfo = dirInfo
			if _, exists := s.records[r.id]; exists {
				return errors.New("transcript: duplicate storage identity")
			}
			s.records[r.id] = r
			s.indexLocked(r)
			s.droppedChunks.Add(r.status.DroppedChunks)
			s.droppedBytes.Add(r.status.DroppedBytes)
		}
		if errRead == io.EOF {
			return nil
		}
		if errRead != nil {
			return errRead
		}
	}
}

func (s *Store) recoverRecord(path string, info os.FileInfo) *record {
	namespace, name, _ := ownedPath(path)
	ext := filepath.Ext(name)
	r := &record{id: strings.TrimSuffix(name, ext), dir: namespace, path: path, fileInfo: info, bytes: info.Size(), retired: true,
		status: Status{State: EndedIncomplete, Ended: info.ModTime(), ApproximateEnd: true}}
	file, err := openOwnedFile(s.root, path)
	if err != nil {
		s.recoveryFailure(r, "recovery-open")
		return r
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		s.recoveryFailure(r, "recovery-open")
		return r
	}
	magic := make([]byte, len(fileMagic))
	if _, err = io.ReadFull(file, magic); err != nil || !bytes.Equal(magic, fileMagic) {
		s.recoveryFailure(r, "recovery-header")
		return r
	}
	r.readable = int64(len(fileMagic))
	first := true
	terminal := false
	valid := true
	var last Status
	for r.readable < info.Size() {
		frame, n, err := decodeFrame(io.LimitReader(file, info.Size()-r.readable))
		if err != nil {
			valid = false
			break
		}
		if first {
			if frame.Kind != "metadata" || frame.Metadata == nil || frame.Status != nil || len(frame.Data) != 0 || !validRecoveredMetadata(*frame.Metadata, namespace) {
				valid = false
				break
			}
			r.metadata = *frame.Metadata
			first = false
		} else if frame.Kind == "metadata" || terminal {
			valid = false
			break
		}
		if frame.Kind == "gap" {
			if frame.Status == nil {
				valid = false
				break
			}
			r.status.DroppedChunks, r.status.DroppedBytes = frame.Status.DroppedChunks, frame.Status.DroppedBytes
		}
		if frame.Kind == "status" {
			if frame.Status == nil || frame.Status.Ended.IsZero() || frame.Status.State != EndedComplete && frame.Status.State != EndedIncomplete {
				valid = false
				break
			}
			last, terminal = *frame.Status, true
		}
		r.readable += n
	}
	if first || !valid {
		s.recoveryFailure(r, "recovery-frame")
	}
	if ext == ".log" && valid && !first && terminal {
		if last.State == EndedComplete && (last.DroppedChunks != 0 || last.DroppedBytes != 0 || last.Failure != "") {
			s.recoveryFailure(r, "recovery-completion")
		} else {
			r.status = last
		}
	} else {
		// An .open can contain a would-be complete terminal frame written
		// before sync, close or rename failed. Never trust it as complete.
		if terminal {
			r.status.DroppedChunks, r.status.DroppedBytes = last.DroppedChunks, last.DroppedBytes
		}
		if ext == ".log" {
			s.recoveryFailure(r, "recovery-terminal")
		}
		s.degraded.Store(true)
	}
	return r
}

func validRecoveredMetadata(metadata Metadata, dir string) bool {
	if metadata.RequestID == "" || metadata.UserID == "" || metadata.Started.IsZero() || !opaque(metadata.SessionID, 64) {
		return false
	}
	if _, err := hex.DecodeString(metadata.SessionID); err != nil {
		return false
	}
	if namespace("user", metadata.UserID, "") != dir {
		return false
	}
	return len(metadata.RequestID)+len(metadata.UserID)+len(metadata.Method)+len(metadata.URL) <= maxMetadataBytes
}

func (s *Store) recoveryFailure(r *record, code string) {
	r.failed = true
	r.status.State = EndedIncomplete
	r.status.Failure = code
	s.failures.Add(1)
	s.degraded.Store(true)
}
