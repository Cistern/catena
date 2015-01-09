package catena

import (
	"errors"
)

var (
	errorReadyOnlyPartition = errors.New("catena: partition is read only")
	errorObservationExists  = errors.New("catena: existing observation in partition")
	errorSourceNotFound     = errors.New("catena: source not found")
	errorMetricNotFound     = errors.New("catena: metric not found")
)

// A partition is a disjoint set of time series
// observations. The timestamp ranges of partitions
// do not overlap.
type partition interface {
	minTimestamp() int64
	maxTimestamp() int64

	readOnly() bool

	addPoints(source, metric string,
		points []point)

	put(Rows) error

	fetchPoints(source, metric string,
		start, end int64) ([]point, error)
}
