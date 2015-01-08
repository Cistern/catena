package catena

import (
	"errors"
)

var (
	ErrorReadyOnlyPartition = errors.New("catena: partition is read only")
	ErrorObservationExists  = errors.New("catena: existing observation in partition")
	ErrorSourceNotFound     = errors.New("catena: source not found")
	ErrorMetricNotFound     = errors.New("catena: metric not found")
)

// A partition is a disjoint set of time series
// observations. The timestamp ranges of partitions
// do not overlap.
type partition interface {
	minTimestamp() int64
	maxTimestamp() int64

	readOnly() bool

	addPoint(source, metric string,
		timestamp int64, value float64) error

	fetchPoints(source, metric string,
		start, end int64) ([]point, error)
}
