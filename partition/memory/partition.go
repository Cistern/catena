package memory

import (
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/Preetam/catena/partition"
	"github.com/Preetam/catena/wal"
)

// MemoryPartition is a partition that exists in-memory.
type MemoryPartition struct {
	readOnly      bool
	partitionLock sync.RWMutex

	minTS int64
	maxTS int64

	sources     map[string]*memorySource
	sourcesLock sync.RWMutex

	wal wal.WAL
}

// memorySource is a source with metrics.
type memorySource struct {
	name    string
	metrics map[string]*memoryMetric
	lock    sync.RWMutex
}

// memoryMetric contains an ordered slice of points.
type memoryMetric struct {
	name   string
	points []partition.Point
	lock   sync.Mutex

	lastInsertIndex int
}

// NewMemoryPartition creates a new MemoryPartition backed by WAL.
func NewMemoryPartition(WAL wal.WAL) *MemoryPartition {
	p := MemoryPartition{
		readOnly: false,
		sources:  map[string]*memorySource{},
		wal:      WAL,
		minTS:    math.MaxInt64,
		maxTS:    math.MinInt64,
	}

	return &p
}

// RecoverMemoryPartition recovers a MemoryPartition backed by WAL.
func RecoverMemoryPartition(WAL wal.WAL) (*MemoryPartition, error) {
	p := &MemoryPartition{
		readOnly: false,
		sources:  map[string]*memorySource{},
		minTS:    math.MaxInt64,
		maxTS:    math.MinInt64,
	}

	var entry wal.WALEntry
	var err error

	for entry, err = WAL.ReadEntry(); err == nil; entry, err = WAL.ReadEntry() {
		p.InsertRows(entry.Rows)
	}

	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}

	err = WAL.Truncate()

	p.wal = WAL

	return p, err
}

// InsertRows inserts rows into the partition.
func (p *MemoryPartition) InsertRows(rows []partition.Row) error {
	if p.readOnly {
		return errors.New("partition/memory: read only")
	}

	if p.wal != nil {
		_, err := p.wal.Append(wal.WALEntry{
			Operation: wal.OperationInsert,
			Rows:      rows,
		})

		if err != nil {
			return err
		}
	}

	var (
		minTS int64
		maxTS int64
	)

	for i, row := range rows {
		if i == 0 {
			minTS = row.Timestamp
			maxTS = row.Timestamp
		}

		if row.Timestamp < minTS {
			minTS = row.Timestamp
		}

		if row.Timestamp > maxTS {
			maxTS = row.Timestamp
		}

		source := p.getOrCreateSource(row.Source)
		metric := source.getOrCreateMetric(row.Metric)
		metric.insertPoints([]partition.Point{row.Point})
	}

	for min := atomic.LoadInt64(&p.minTS); min > minTS; min = atomic.LoadInt64(&p.minTS) {
		if atomic.CompareAndSwapInt64(&p.minTS, min, minTS) {
			break
		}
	}

	for max := atomic.LoadInt64(&p.maxTS); max < maxTS; max = atomic.LoadInt64(&p.maxTS) {
		if atomic.CompareAndSwapInt64(&p.maxTS, max, maxTS) {
			break
		}
	}

	return nil
}

// SetReadOnly sets the partition mode to read-only.
func (m *MemoryPartition) SetReadOnly() {
	m.readOnly = true
}

// Closes sets the memory partition to read-only, releases resources,
// and closes its WAL.
func (m *MemoryPartition) Close() error {
	// Close WAL
	err := m.wal.Close()
	if err != nil {
		return err
	}

	m.readOnly = true

	m.sources = nil

	return nil
}

func (p *MemoryPartition) MinTimestamp() int64 {
	return atomic.LoadInt64(&p.minTS)
}

func (p *MemoryPartition) MaxTimestamp() int64 {
	return atomic.LoadInt64(&p.maxTS)
}

func (p *MemoryPartition) ReadOnly() bool {
	return p.readOnly
}

func (p *MemoryPartition) Filename() string {
	return p.wal.Filename()
}

func (p *MemoryPartition) Sources() []string {
	sources := []string{}
	for source := range p.sources {
		sources = append(sources, source)
	}

	return sources
}

func (p *MemoryPartition) Metrics(source string) []string {
	metrics := []string{}

	src, present := p.sources[source]
	if !present {
		return metrics
	}

	for metric := range src.metrics {
		metrics = append(metrics, metric)
	}

	return metrics
}

func (p *MemoryPartition) HasSource(source string) bool {
	_, present := p.sources[source]
	return present
}

func (p *MemoryPartition) HasMetric(source, metric string) bool {
	src, present := p.sources[source]
	if !present {
		return false
	}

	_, present = src.metrics[metric]
	return present
}

func (p *MemoryPartition) Hold() {
	p.partitionLock.RLock()
}

func (p *MemoryPartition) Release() {
	p.partitionLock.RUnlock()
}

func (p *MemoryPartition) ExclusiveHold() {
	p.partitionLock.Lock()
}

func (p *MemoryPartition) ExclusiveRelease() {
	p.partitionLock.Unlock()
}

// Destroy destroys the memory partition as well as its WAL.
func (m *MemoryPartition) Destroy() error {
	// Destroy WAL
	err := m.wal.Destroy()

	m.readOnly = true

	return err
}
