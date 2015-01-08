package catena

import (
	"sync"
)

// memoryPartition represents a catena partition
// stored in memory.
type memoryPartition struct {
	// Metadata
	minTS int64
	maxTS int64
	ro    bool

	sources []memorySource

	// Write-ahead log
	log wal

	lock sync.RWMutex
}

// memorySource is a metric source registered in memory.
type memorySource struct {
	name    string
	metrics []memoryMetric
}

// memoryMetric is a metric in memory.
type memoryMetric struct {
	name   string
	points []point
}

func newMemoryPartition() *memoryPartition {
	return &memoryPartition{}
}

// minTimestamp returns the minimum timestamp
// value held by this partition.
func (p *memoryPartition) minTimestamp() int64 {
	return p.minTS
}

// maxTimestamp returns the maximum timestamp
// value held by this partition.
func (p *memoryPartition) maxTimestamp() int64 {
	return p.maxTS
}

// readOnly returns whether this partition is read only.
func (p *memoryPartition) readOnly() bool {
	return p.ro
}

// addPoint adds a point to the partition and returns
// an error.
func (p *memoryPartition) addPoint(source, metric string,
	timestamp int64, value float64) error {

	// Lock for writing
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.ro {
		return ErrorReadyOnlyPartition
	}

	// Write to the WAL.

	if timestamp > p.maxTS {
		p.maxTS = timestamp
	}

	switch {
	case len(p.sources) == 0:
		// This is the first point.
		p.minTS = timestamp
		p.maxTS = timestamp
	case timestamp < p.minTS:
		p.minTS = timestamp
	case timestamp > p.maxTS:
		p.maxTS = timestamp
	}

	// What follow are in-order insertions
	// into the sources, metrics, and points
	// slices.

	sourceIndex := -1
	metricIndex := -1

	insertionPoint := -1

	// TODO: use a binary search
	for i, src := range p.sources {
		if src.name == source {
			sourceIndex = i
			break
		}

		if src.name > source {
			insertionPoint = i
		}
	}

	var src memorySource

	if sourceIndex == -1 {
		// memorySource was not found.
		src.name = source

		if insertionPoint == -1 {
			// Insert at the end.
			insertionPoint = len(p.sources)
		}

		// Update the sources slice.
		p.sources = append(p.sources[:insertionPoint],
			append([]memorySource{src},
				p.sources[insertionPoint:]...,
			)...,
		)

		sourceIndex = insertionPoint
	}

	src = p.sources[sourceIndex]

	// Now we do essentially the same
	// thing for the metric.
	insertionPoint = -1

	for i, met := range src.metrics {
		if met.name == metric {
			metricIndex = i
			break
		}

		if met.name > metric {
			insertionPoint = i
		}
	}

	var met memoryMetric

	if metricIndex == -1 {
		// memoryMetric was not found.
		met.name = metric

		if insertionPoint == -1 {
			// Insert at the end.
			insertionPoint = len(src.metrics)
		}

		// Update the metrics slice.
		src.metrics = append(src.metrics[:insertionPoint],
			append([]memoryMetric{met},
				src.metrics[insertionPoint:]...,
			)...,
		)

		metricIndex = insertionPoint
	}

	met = src.metrics[metricIndex]

	// Insert the point in order.
	insertionPoint = -1

	// We start from the end because the expected
	// case is appending to the end.
	lastIndex := len(met.points) - 1
	for i := lastIndex; i >= 0; i-- {

		if met.points[i].Timestamp == timestamp {
			return ErrorObservationExists
		}

		if met.points[i].Timestamp < timestamp {
			insertionPoint = i
			break
		}
	}

	if insertionPoint == -1 {
		insertionPoint = lastIndex + 1
	}

	newPoint := point{
		Timestamp: timestamp,
		Value:     value,
	}

	// Update the points slice.
	met.points = append(met.points[:insertionPoint],
		append([]point{newPoint},
			met.points[insertionPoint:]...,
		)...,
	)

	// Update original structures.
	src.metrics[metricIndex] = met
	p.sources[sourceIndex] = src

	return nil
}

// fetchPoints returns an ordered slice of points for the
// (source, metric) series within the [start, end] time range.
func (p *memoryPartition) fetchPoints(source, metric string,
	start, end int64) ([]point, error) {

	// Lock for reading
	p.lock.RLock()
	defer p.lock.RUnlock()

	points := []point{}

	timeRange := end - start

	// Iterate through sources
	for _, src := range p.sources {
		if src.name == source {

			// Iterate through metrics
			for _, met := range src.metrics {
				if met.name == metric {

					// Iterate through points
					for _, point := range met.points {
						if point.Timestamp-start <= timeRange {
							points = append(points, point)
						}

						if point.Timestamp > end {
							break
						}
					}
					break
				}

				if met.name > metric {
					return nil, ErrorMetricNotFound
				}
			}
			break
		}

		if src.name > source {
			return nil, ErrorSourceNotFound
		}
	}

	return points, nil
}

// memoryPartition is a partition
var _ partition = &memoryPartition{}
