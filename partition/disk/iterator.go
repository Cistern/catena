package disk

import (
	"errors"

	"github.com/Preetam/catena/partition"
)

// NewIterator returns an Iterator for the partition that iterates over
// a sequence of points for the given source and metric.
func (p *DiskPartition) NewIterator(sourceName string, metricName string) (partition.Iterator, error) {
	p.Hold()
	i := &diskIterator{}

	source, present := p.sources[sourceName]
	if !present {
		p.Release()
		return nil, errors.New("partition/disk: source not found")
	}

	metric, present := source.metrics[metricName]
	if !present {
		p.Release()
		return nil, errors.New("partition/disk: metric not found")
	}

	i.metric = metric
	i.partition = p
	i.sourceName = sourceName

	err := i.Reset()
	if err != nil {
		p.Release()
		return nil, err
	}

	i.currentPointIndex = -1

	return i, nil
}

type diskIterator struct {
	sourceName          string
	partition           *DiskPartition
	metric              diskMetric
	currentExtent       diskExtent
	currentExtentIndex  int
	currentExtentPoints []partition.Point
	currentPointIndex   int
	currentPoint        partition.Point
}

// Point returns the current point.
func (i *diskIterator) Point() partition.Point {
	return i.currentPoint
}

// Reset moves the iterator to the first available point.
func (i *diskIterator) Reset() error {
	i.currentExtent = i.metric.extents[0]
	i.currentExtentIndex = 0

	points, err := i.partition.extentPoints(i.currentExtent)
	if err != nil {
		return err
	}

	i.currentExtentPoints = points
	i.currentPoint = points[0]
	i.currentPointIndex = 0

	return nil
}

// Seek moves the iterator to the first timestamp greater
// than or equal to the given timestamp.
func (i *diskIterator) Seek(timestamp int64) error {
	i.Reset()

	for {
		firstTSInExtent := i.currentExtent.startTS
		lastTSInExtent := i.currentExtentPoints[len(i.currentExtentPoints)-1].Timestamp

		if firstTSInExtent >= timestamp {
			i.currentPointIndex = 0
			i.currentPoint = i.currentExtentPoints[0]
			return nil
		}

		if firstTSInExtent < timestamp && lastTSInExtent < timestamp {
			err := i.nextExtent()
			if err != nil {
				return err
			}

			continue
		}

		for index := i.currentPointIndex; index < len(i.currentExtentPoints); index++ {
			if i.currentExtentPoints[index].Timestamp >= timestamp {
				i.currentPointIndex = index
				i.currentPoint = i.currentExtentPoints[index]
				return nil
			}
		}
	}

	return errors.New("partition/disk: could not seek to requested timestamp")
}

// Next moves the iterator to the next point.
func (i *diskIterator) Next() error {
	i.currentPointIndex++
	if i.currentPointIndex == len(i.currentExtentPoints) {
		return i.nextExtent()
	}

	i.currentPoint = i.currentExtentPoints[i.currentPointIndex]

	return nil
}

// Close closes the iterator.
func (i *diskIterator) Close() {
	i.partition.Release()
	i.currentExtentPoints = nil
}

func (i *diskIterator) nextExtent() error {
	var err error

	if i.currentExtentIndex == len(i.metric.extents)-1 {
		return errors.New("partition/disk: no more extents")
	}

	i.currentExtentIndex++
	i.currentExtent = i.metric.extents[i.currentExtentIndex]
	i.currentExtentPoints, err = i.partition.extentPoints(i.currentExtent)
	if err != nil {
		return err
	}

	i.currentPointIndex = 0
	i.currentPoint = i.currentExtentPoints[0]

	return nil
}

// diskIterator is an Iterator.
var _ partition.Iterator = &diskIterator{}
