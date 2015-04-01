package memory

import (
	"fmt"

	"github.com/PreetamJinka/catena/partition"
)

func (p *MemoryPartition) getOrCreateSource(name string) *memorySource {
	var source *memorySource
	present := false

	p.sourcesLock.RLock()
	if source, present = p.sources[name]; !present {
		p.sourcesLock.RUnlock()
		p.sourcesLock.Lock()

		if source, present = p.sources[name]; !present {
			source = &memorySource{
				name:    name,
				metrics: map[string]*memoryMetric{},
			}

			p.sources[name] = source
		}

		p.sourcesLock.Unlock()
	} else {
		p.sourcesLock.RUnlock()
	}

	return source
}

func (s *memorySource) getOrCreateMetric(name string) *memoryMetric {
	var metric *memoryMetric
	present := false

	s.lock.RLock()
	if metric, present = s.metrics[name]; !present {
		s.lock.RUnlock()
		s.lock.Lock()

		if metric, present = s.metrics[name]; !present {
			metric = &memoryMetric{
				name:            name,
				points:          make([]partition.Point, 0, 64),
				lastInsertIndex: -1,
			}

			s.metrics[name] = metric
		}

		s.lock.Unlock()
	} else {
		s.lock.RUnlock()
	}

	return metric
}

func (m *memoryMetric) insertPoints(points []partition.Point) {
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

func (m *memoryMetric) insertAfter(point partition.Point, after int) int {
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
