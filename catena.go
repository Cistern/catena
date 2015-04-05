// Package catena provides a time series storage engine.
package catena

import (
	"github.com/PreetamJinka/catena/partition"
	"github.com/PreetamJinka/catena/partition/disk"
	"github.com/PreetamJinka/catena/partition/memory"
)

// A Point is a single observation of a time series metric. It
// has a timestamp and a value.
type Point struct {
	Timestamp int64
	Value     float64
}

// A Row is a Point with Source and Metric fields.
type Row struct {
	Source string
	Metric string
	Point
}

// Making sure there are no import cycles
var _ partition.Partition = &disk.DiskPartition{}
var _ partition.Partition = &memory.MemoryPartition{}
var _ partition.Iterator
