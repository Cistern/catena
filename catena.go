// Package catena provides a time series storage engine.
package catena

import (
	"github.com/PreetamJinka/catena/partition"
	"github.com/PreetamJinka/catena/partition/disk"
	"github.com/PreetamJinka/catena/partition/memory"
)

// A Point is a single observation of a time series metric. It
// has a timestamp and a value.
type Point partition.Point

// A Row is a Point with Source and Metric fields.
type Row partition.Row

// NewRow returns a new Row with the given values.
func NewRow(source, metric string, timestamp int64, value float64) Row {
	return Row{
		Source: source,
		Metric: metric,
		Point: partition.Point{
			Timestamp: timestamp,
			Value:     value,
		},
	}
}

// NewPoint returns a new Point with the given values.
func NewPoint(timestamp int64, value float64) Point {
	return Point{
		Timestamp: timestamp,
		Value:     value,
	}
}

// Making sure there are no import cycles
var _ partition.Partition = &disk.DiskPartition{}
var _ partition.Partition = &memory.MemoryPartition{}
var _ partition.Iterator
