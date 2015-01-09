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

	sources []*memorySource

	walLock sync.Mutex
	// Write-ahead log
	log wal

	// Buffered channel of writes.
	pendingWrites chan Rows

	lock sync.RWMutex
}

// memorySource is a metric source registered in memory.
type memorySource struct {
	name    string
	metrics []*memoryMetric

	lock sync.RWMutex
}

// memoryMetric is a metric in memory.
type memoryMetric struct {
	name   string
	points []point

	lock sync.RWMutex
}

// newMemoryPartition creates a new memoryPartition.
func newMemoryPartition(log wal) (*memoryPartition, error) {
	p := &memoryPartition{
		log:           log,
		pendingWrites: make(chan Rows),
	}

	go p.handleWrites()

	// Load data from the WAL.
	for entry, err := log.readEntry(); err == nil; entry, err = log.readEntry() {
		if entry.operation == operationInsert {
			p.pendingWrites <- entry.rows
		}
	}

	// Truncate WAL.
	err := log.truncate()
	if err != nil {
		return nil, err
	}

	return p, nil
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

// put adds rows to a partition.
func (p *memoryPartition) put(rows Rows) error {
	if p.ro {
		return errorReadyOnlyPartition
	}

	// Grab lock for the WAL.
	p.walLock.Lock()
	_, err := p.log.append(walEntry{
		operation: operationInsert,
		rows:      rows,
	})

	if err != nil {
		// Something went wrong writing to the WAL.
		// Truncate to prepare for future writes.
		_ = p.log.truncate()
		// TODO: What happens if we can't truncate?!

		// Release WAL lock.
		p.walLock.Unlock()
		return err
	}

	// Successfully wrote to the WAL.
	// Buffer the writes.
	p.pendingWrites <- rows

	// Writes don't return errors.
	return nil
}

func (p *memoryPartition) addPoints(source, metric string, points []point) {
	p.lock.Lock()

	if len(p.sources) == 0 {
		// This is the first point.
		if len(points) > 0 {
			p.minTS = points[0].Timestamp
			p.maxTS = points[0].Timestamp
		}
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

	var src *memorySource

	if sourceIndex == -1 {
		// memorySource was not found.
		src = &memorySource{}
		src.name = source

		if insertionPoint == -1 {
			// Insert at the end.
			insertionPoint = len(p.sources)
		}

		// Update the sources slice.
		p.sources = append(p.sources[:insertionPoint],
			append([]*memorySource{src},
				p.sources[insertionPoint:]...,
			)...,
		)

		sourceIndex = insertionPoint
	}

	src = p.sources[sourceIndex]

	p.lock.Unlock()
	src.lock.Lock()

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

	var met *memoryMetric

	if metricIndex == -1 {
		// memoryMetric was not found.
		met = &memoryMetric{}
		met.name = metric

		if insertionPoint == -1 {
			// Insert at the end.
			insertionPoint = len(src.metrics)
		}

		// Update the metrics slice.
		src.metrics = append(src.metrics[:insertionPoint],
			append([]*memoryMetric{met},
				src.metrics[insertionPoint:]...,
			)...,
		)

		metricIndex = insertionPoint
	}

	met = src.metrics[metricIndex]

	src.lock.Unlock()
	met.lock.Lock()

	// Insert the points in order.

NEXT_POINT:
	for _, newPoint := range points {
		timestamp := newPoint.Timestamp
		value := newPoint.Value

		switch {
		case timestamp < p.minTS:
			p.minTS = timestamp
		case timestamp > p.maxTS:
			p.maxTS = timestamp
		}

		insertionPoint = -1

		// We start from the end because the expected
		// case is appending to the end.
		lastIndex := len(met.points) - 1
		for i := lastIndex; i >= 0; i-- {

			ts := met.points[i].Timestamp

			switch {
			case ts == timestamp:

				// We *must* unlock before returning!
				met.lock.Unlock()
				met.points[i].Value = value
				goto NEXT_POINT

			case ts > timestamp:
				insertionPoint = i

			default:
				break
			}

		}

		if insertionPoint == -1 {
			insertionPoint = lastIndex + 1
		}

		// Update the points slice.
		met.points = append(met.points[:insertionPoint],
			append([]point{newPoint},
				met.points[insertionPoint:]...,
			)...,
		)
	}

	met.lock.Unlock()
}

// fetchPoints returns an ordered slice of points for the
// (source, metric) series within the [start, end] time range.
func (p *memoryPartition) fetchPoints(source, metric string,
	start, end int64) ([]point, error) {

	p.lock.RLock()

	if len(p.sources) == 0 {
		return nil, errorSourceNotFound
	}

	points := []point{}

	timeRange := end - start

	// Iterate through sources

	for _, src := range p.sources {
		if src.name == source {
			p.lock.RUnlock()

			// Iterate through metrics
			src.lock.RLock()
			for _, met := range src.metrics {
				if met.name == metric {
					src.lock.RUnlock()

					// Iterate through points
					met.lock.RLock()
					for _, point := range met.points {
						if point.Timestamp-start < 0 {
							continue
						}

						if point.Timestamp-start <= timeRange {
							points = append(points, point)
						}

						if point.Timestamp > end {
							break
						}
					}
					met.lock.RUnlock()
					break
				}

				if met.name > metric {
					src.lock.RUnlock()
					return nil, errorMetricNotFound
				}
			}
			break
		}

		if src.name > source {
			p.lock.RUnlock()
			return nil, errorSourceNotFound
		}
	}

	return points, nil
}

func (p *memoryPartition) handleWrites() {
	for rows := range p.pendingWrites {
		sources := map[string]map[string][]point{}

		for _, row := range rows {
			metrics := sources[row.Source]

			if metrics == nil {
				metrics = map[string][]point{}
			}

			metrics[row.Metric] = append(metrics[row.Metric],
				point{
					Timestamp: row.Timestamp,
					Value:     row.Value,
				})

			sources[row.Source] = metrics
		}

		for source, metrics := range sources {
			for metric, points := range metrics {
				p.addPoints(source, metric, points)
			}
		}
	}
}

// memoryPartition is a partition
var _ partition = &memoryPartition{}
