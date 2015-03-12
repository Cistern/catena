// Package catena provides a time series storage engine.
package catena

import (
	"github.com/PreetamJinka/catena/partition"
	"github.com/PreetamJinka/catena/partition/disk"
	"github.com/PreetamJinka/catena/partition/iterator"
	"github.com/PreetamJinka/catena/partition/memory"
)

type Point partition.Point

type Row partition.Row

var _ = &disk.DiskPartition{}
var _ = &memory.MemoryPartition{}
var _ iterator.Iterator
