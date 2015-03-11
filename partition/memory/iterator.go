package memory

import (
	"errors"

	"github.com/PreetamJinka/catena"
	"github.com/PreetamJinka/catena/partition"
)

// NewIterator returns an Iterator for the partition that iterates over
// a sequence of points for the given source and metric.
func (p *MemoryPartition) NewIterator(sourceName string, metricName string) (*memoryIterator, error) {
	p.partitionLock.RLock()

	p.sourcesLock.Lock()
	source, present := p.sources[sourceName]
	if !present {
		return nil, errors.New("partition/memory: source not found")
	}

	metric, present := source.metrics[metricName]
	if !present {
		return nil, errors.New("partition/memory: metric not found")
	}
	p.sourcesLock.Unlock()

	return &memoryIterator{
		sourceName:   sourceName,
		partition:    p,
		metric:       metric,
		currentIndex: -1,
	}, nil
}

type memoryIterator struct {
	sourceName   string
	partition    *MemoryPartition
	metric       *memoryMetric
	currentIndex int
	currentPoint catena.Point
}

// Point returns the current point.
func (i *memoryIterator) Point() catena.Point {
	return i.currentPoint
}

// Reset moves the iterator to the first available point.
func (i *memoryIterator) Reset() error {
	i.currentIndex = 0

	i.metric.lock.Lock()
	defer i.metric.lock.Unlock()

	i.currentPoint = i.metric.points[0]

	return nil
}

// Seek moves the iterator to the first timestamp greater
// than or equal to the given timestamp.
func (i *memoryIterator) Seek(timestamp int64) error {
	i.currentIndex = 0

	i.metric.lock.Lock()
	defer i.metric.lock.Unlock()

	for ; i.currentIndex != len(i.metric.points); i.currentIndex++ {
		if i.metric.points[i.currentIndex].Timestamp >= timestamp {
			break
		}
	}

	i.currentPoint = i.metric.points[i.currentIndex]
	if i.currentPoint.Timestamp < timestamp {
		return errors.New("partition/memory: could not seek to requested timestamp")
	}

	return nil
}

// Next moves the iterator to the next point.
func (i *memoryIterator) Next() error {
	if i.currentIndex < 0 {
		i.Reset()
		return nil
	}

	i.metric.lock.Lock()
	defer i.metric.lock.Unlock()

	if i.metric.points[i.currentIndex] != i.currentPoint {
		for i.currentIndex = 0; i.currentIndex != len(i.metric.points); i.currentIndex++ {
			if i.metric.points[i.currentIndex] == i.currentPoint {
				break
			}
		}
	}

	if i.currentIndex == len(i.metric.points)-1 {
		return errors.New("partition/memory: iterator index out of bounds")
	}

	i.currentIndex++
	i.currentPoint = i.metric.points[i.currentIndex]

	return nil
}

// Close closes the iterator.
func (i *memoryIterator) Close() {
	i.partition.partitionLock.RUnlock()
}

// memoryIterator is an Iterator.
var _ partition.Iterator = &memoryIterator{}
