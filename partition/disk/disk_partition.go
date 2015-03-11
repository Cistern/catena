package disk

import (
	"os"
)

// diskPartition represents a partition
// stored as a file on disk.
type DiskPartition struct {
	// Metadata
	minTS int64
	maxTS int64

	// File on disk
	f *os.File

	// Memory mapped backed by f
	mapped []byte

	sources map[string]diskSource
}

// diskSource is a metric source registered on disk.
type diskSource struct {
	name    string
	metrics map[string]diskMetric
}

// diskMetric is a metric on disk.
type diskMetric struct {
	name      string
	offset    int64
	numPoints int
}
