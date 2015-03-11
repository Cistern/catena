package partition

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/PreetamJinka/catena"
	"github.com/PreetamJinka/catena/wal"
)

// MemoryPartition is a partition that exists in-memory.
type MemoryPartition struct {
	readOnly      bool
	partitionLock sync.RWMutex

	sources     map[string]*memorySource
	sourcesLock sync.Mutex

	wal wal.WAL
}

// memorySource is a source with metrics.
type memorySource struct {
	name    string
	metrics map[string]*memoryMetric
	lock    sync.Mutex
}

// memoryMetric contains an ordered slice of points.
type memoryMetric struct {
	name   string
	points []catena.Point
	lock   sync.Mutex

	lastInsertIndex int
}

// NewMemoryPartition creates a new MemoryPartition backed by WAL.
func NewMemoryPartition(WAL wal.WAL) *MemoryPartition {
	p := MemoryPartition{
		readOnly: false,
		sources:  map[string]*memorySource{},
		wal:      WAL,
	}

	return &p
}

// RecoverMemoryPartition recovers a MemoryPartition backed by WAL.
func RecoverMemoryPartition(WAL wal.WAL) (*MemoryPartition, error) {
	p := &MemoryPartition{
		readOnly: false,
		sources:  map[string]*memorySource{},
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
func (p *MemoryPartition) InsertRows(rows []catena.Row) error {
	p.partitionLock.RLock()
	if p.readOnly {
		return errors.New("partition: read only")
	}

	if p.wal != nil {
		_, err := p.wal.Append(wal.WALEntry{
			Operation: wal.OperationInsert,
			Rows:      rows,
		})

		if err != nil {
			p.partitionLock.RUnlock()
			return err
		}
	}
	p.partitionLock.RUnlock()

	for _, row := range rows {
		source := p.getOrCreateSource(row.Source)
		metric := source.getOrCreateMetric(row.Metric)
		metric.insertPoints([]catena.Point{row.Point})
	}

	return nil
}

// Destroy destroys the memory partition as well as its WAL.
func (m *MemoryPartition) Destroy() error {
	m.partitionLock.Lock()

	// Destroy WAL
	err := m.wal.Destroy()

	m.readOnly = true
	m.partitionLock.Unlock()

	return err
}

func (p *MemoryPartition) getOrCreateSource(name string) *memorySource {
	var source *memorySource
	present := false

	if source, present = p.sources[name]; !present {
		p.sourcesLock.Lock()

		if source, present = p.sources[name]; !present {
			source = &memorySource{
				name:    name,
				metrics: map[string]*memoryMetric{},
			}

			p.sources[name] = source
		}

		p.sourcesLock.Unlock()
	}

	return source
}

func (s *memorySource) getOrCreateMetric(name string) *memoryMetric {
	var metric *memoryMetric
	present := false

	if metric, present = s.metrics[name]; !present {
		s.lock.Lock()

		if metric, present = s.metrics[name]; !present {
			metric = &memoryMetric{
				name:            name,
				points:          make([]catena.Point, 0, 64),
				lastInsertIndex: -1,
			}

			s.metrics[name] = metric
		}

		s.lock.Unlock()
	}

	return metric
}

func (m *memoryMetric) insertPoints(points []catena.Point) {
	m.lock.Lock()

	for _, point := range points {
		if m.lastInsertIndex < 0 {
			// This is the first point.
			m.points = append(m.points, point)
			m.lastInsertIndex = 0
			continue
		}

		prevPoint := m.points[m.lastInsertIndex]
		if prevPoint.Timestamp < point.Timestamp {
			m.lastInsertIndex = m.insertAfter(point, m.lastInsertIndex)
			continue
		}

		var i int
		for i = m.lastInsertIndex; i > 0 && m.points[i].Timestamp > point.Timestamp; i-- {
		}

		m.lastInsertIndex = m.insertAfter(point, i)
	}

	m.lock.Unlock()
}

func (m *memoryMetric) insertAfter(point catena.Point, after int) int {
	lenPoints := len(m.points)

	var i int
	for i = after; i < lenPoints && m.points[i].Timestamp < point.Timestamp; i++ {
	}

	// Resize
	m.points = append(m.points, point)

	// Shift elements over
	copy(m.points[i+1:], m.points[i:lenPoints])

	// Insert in place
	m.points[i] = point

	return i
}

func (s *memorySource) String() string {
	str := ""

	str += fmt.Sprintf("source %s\n", s.name)
	for _, metric := range s.metrics {
		str += metric.String()
	}

	return str
}

func (m *memoryMetric) String() string {
	str := ""

	str += fmt.Sprintf("  metric %s\n", m.name)
	for _, p := range m.points {
		str += fmt.Sprintf("    %d => %g\n", p.Timestamp, p.Value)
	}

	return str
}
