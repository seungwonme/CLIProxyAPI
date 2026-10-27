// Package core implements bounded, best-effort local request transcripts.
package core

import (
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultMaxBytes int64 = 10 << 30

const (
	Active          = "ACTIVE"
	EndedIncomplete = "ENDED_INCOMPLETE"
	EndedComplete   = "ENDED_COMPLETE"
)

// Config controls aggregate logical storage and bounded in-memory admission.
// Zero limits select defaults. An empty Root disables the store.
type Config struct {
	Root       string
	MaxBytes   int64
	MaxActive  int
	QueueBytes int64
	MaxRecords int
}

type Metadata struct {
	RequestID string
	UserID    string
	SessionID string
	Method    string
	URL       string
	Headers   map[string][]string
	Started   time.Time
}

type Frame struct {
	Kind     string
	At       time.Time
	Data     []byte
	Metadata *Metadata `json:",omitempty"`
	Status   *Status   `json:",omitempty"`
}

type Status struct {
	State          string
	DroppedChunks  uint64
	DroppedBytes   uint64
	Ended          time.Time
	ApproximateEnd bool
	Failure        string
}

type Stats struct {
	LogicalBytes  int64
	ReservedBytes int64
	LimitBytes    int64
	Active        int
	Records       int
	DroppedChunks uint64
	DroppedBytes  uint64
	Skipped       uint64
	Failures      uint64
	Degraded      bool
}

const (
	maxChunkBytes           = 32 << 10
	maxFrameBytes           = 128 << 10
	maxMetadataBytes        = 32 << 10
	maxKindBytes            = 128
	terminalReserve  int64  = 4096
	queueOverhead    int64  = 256
	endFlag          uint64 = 1 << 63
	completeFlag     uint64 = 1 << 62
	captureMask      uint64 = completeFlag - 1
)

// faultHooks is installed by package tests before starting the worker. Hooks
// replace an operation, so a short-write hook must actually write its prefix.
// beforeWorker may block to deterministically saturate admission and data queues.
type faultHooks struct {
	write        func(*os.File, []byte) (int, error)
	sync         func(*os.File) error
	close        func(*os.File) error
	rename       func(*os.Root, string, string) error
	unlink       func(*os.Root, string) error
	beforeWorker func()
}

type Store struct {
	cfg           Config
	root          *os.Root
	rootLock      *os.File
	rootInfo      os.FileInfo
	hooks         *faultHooks
	accepting     atomic.Bool
	stopping      atomic.Bool
	closed        atomic.Bool
	beginning     atomic.Int64
	active        atomic.Int64
	queuedBytes   atomic.Int64
	droppedChunks atomic.Uint64
	droppedBytes  atomic.Uint64
	skipped       atomic.Uint64
	failures      atomic.Uint64
	degraded      atomic.Bool
	admit         chan *Handle
	queue         chan capture
	wake          chan struct{}
	done          chan struct{}
	slots         chan struct{}
	// mu protects bounded indexes, pins and logical/reserved accounting. No
	// inference method takes it, and the worker never holds it during I/O.
	mu       sync.Mutex
	records  map[string]*record
	latest   map[string]*record
	readers  int
	logical  int64
	reserved int64
	closeErr error
}

// Handle contains an independent terminal publication and loss counters. It is
// never recycled, so late callbacks cannot mutate a later request's state.
type Handle struct {
	store         *Store
	metadata      Metadata
	state         atomic.Uint64
	endedAt       atomic.Int64
	pending       atomic.Int64
	droppedChunks atomic.Uint64
	droppedBytes  atomic.Uint64
}

type capture struct {
	handle *Handle
	kind   string
	at     time.Time
	data   []byte
	charge int64
}

type record struct {
	id             string
	path           string
	dir            string
	dirInfo        os.FileInfo
	fileInfo       os.FileInfo
	metadata       Metadata
	handle         *Handle
	file           *os.File
	bytes          int64
	reserved       int64
	readable       int64
	status         Status
	retired        bool
	deleting       bool
	pins           int
	failed         bool
	reportedChunks uint64
	reportedBytes  uint64
}

// Reader pins an entire request until Close. Next is bounded by maxFrameBytes.
// A reader is a snapshot of committed frames; reopen to observe later appends.
type Reader struct {
	mu        sync.Mutex
	store     *Store
	record    *record
	file      *os.File
	remaining int64
	name      string
	status    Status
	closed    bool
}
