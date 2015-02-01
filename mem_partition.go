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

	sources *sourcelist

	walLock sync.Mutex
	// Write-ahead log
	log wal

	lock sync.RWMutex
}

// memorySource is a metric source registered in memory.
type memorySource struct {
	name    string
	metrics *metriclist
}

// memoryMetric is a metric in memory.
type memoryMetric struct {
	name   string
	points *pointList
}

// newMemoryPartition creates a new memoryPartition.
func newMemoryPartition(log wal) (*memoryPartition, error) {
	p := &memoryPartition{
		log:     log,
		sources: Newsourcelist(),
	}

	// Load data from the WAL.
	for entry, err := log.readEntry(); err == nil; entry, err = log.readEntry() {
		if entry.operation == operationInsert {
			sources := map[string]map[string][]Point{}

			for _, row := range entry.rows {
				metrics := sources[row.Source]

				if metrics == nil {
					metrics = map[string][]Point{}
				}

				metrics[row.Metric] = append(metrics[row.Metric],
					Point{
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
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.minTS
}

// maxTimestamp returns the maximum timestamp
// value held by this partition.
func (p *memoryPartition) maxTimestamp() int64 {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.maxTS
}

// readOnly returns whether this partition is read only.
func (p *memoryPartition) readOnly() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.ro
}

// setReadOnly sets the the readOnly flag.
func (p *memoryPartition) setReadOnly() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.ro = true
}

// filename returns the filename of the WAL used
// by this partition.
func (p *memoryPartition) filename() string {
	return p.log.filename()
}

// put adds rows to a partition.
func (p *memoryPartition) put(rows Rows) error {
	p.lock.Lock()

	if p.ro {
		return errorReadyOnlyPartition
	}

	_, err := p.log.append(walEntry{
		operation: operationInsert,
		rows:      rows,
	})

	if err != nil {
		// Something went wrong writing to the WAL.
		// Truncate to prepare for future writes.
		_ = p.log.truncate()
		// TODO: What happens if we can't truncate?!

		return err
	}

	p.lock.Unlock()

	sources := map[string]map[string][]Point{}

	for _, row := range rows {
		metrics := sources[row.Source]

		if metrics == nil {
			metrics = map[string][]Point{}
		}

		metrics[row.Metric] = append(metrics[row.Metric],
			Point{
				Timestamp: row.Timestamp,
				Value:     row.Value,
			})

		sources[row.Source] = metrics
	}

	wg := sync.WaitGroup{}
	for source, metrics := range sources {
		for metric, points := range metrics {
			wg.Add(1)
			go func(s, m string, pnts []Point) {
				p.addPoints(s, m, pnts)
				wg.Done()
			}(source, metric, points)
		}
	}
	wg.Wait()

	// Writes don't return errors.
	return nil
}

// addPoints adds points to the partition.
func (p *memoryPartition) addPoints(source, metric string, points []Point) {
	var err error

	if p.sources.Size() == 0 {
		// This is the first point.
		if len(points) > 0 {
			p.minTS = points[0].Timestamp
			p.maxTS = points[0].Timestamp
		}
	}

	var src *memorySource

	sourceIter := p.sources.NewIterator()
	for sourceIter.Next() {
		v, err := sourceIter.Value()
		if err == nil && v.name == source {
			src = v
		}

		if v.name > source {
			break
		}
	}

	if src == nil {
		src = &memorySource{
			name:    source,
			metrics: Newmetriclist(),
		}

		err = p.sources.Insert(src)
		if err != nil {
			logger.Println(err)

			for sourceIter.Next() {
				v, err := sourceIter.Value()
				if err != nil && v.name == source {
					src = v
				}
			}
		}
	}

	var met *memoryMetric

	metricIter := src.metrics.NewIterator()
	for metricIter.Next() {
		v, err := metricIter.Value()
		if err == nil && v.name == metric {
			met = v
		}
	}

	if met == nil {
		met = &memoryMetric{
			name:   metric,
			points: NewpointList(),
		}

		err = src.metrics.Insert(met)
		if err != nil {
			logger.Println(err)

			for metricIter.Next() {
				v, err := metricIter.Value()
				if err != nil && v.name == metric {
					met = v
				}
			}
		}
	}

	// Insert the points in order.
	for _, newPoint := range points {
		timestamp := newPoint.Timestamp

		switch {
		case timestamp < p.minTS:
			p.minTS = timestamp
		case timestamp > p.maxTS:
			p.maxTS = timestamp
		}

		err = met.points.Insert(newPoint)
		if err != nil {
			logger.Println(err)
		}
	}
}

// fetchPoints returns an ordered slice of points for the
// (source, metric) series within the [start, end] time range.
func (p *memoryPartition) fetchPoints(source, metric string,
	start, end int64) ([]Point, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.sources.Size() == 0 {
		return nil, errorSourceNotFound
	}

	points := []Point{}

	timeRange := end - start

	// Iterate through sources
	sourceIter := p.sources.NewIterator()
	for sourceIter.Next() {
		src, err := sourceIter.Value()
		if err != nil {
			return nil, err
		}

		if src.name > source {
			return nil, errorSourceNotFound
		}

		if src.name == source {

			// Iterate through metrics
			metricIter := src.metrics.NewIterator()
			for metricIter.Next() {
				met, err := metricIter.Value()
				if err != nil {
					return nil, err
				}

				if met.name > metric {
					return nil, errorMetricNotFound
				}

				if met.name == metric {
					// Iterate through points
					pointIter := met.points.NewIterator()
					for pointIter.Next() {
						point, err := pointIter.Value()
						if err != nil {
							return nil, err
						}

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

					break
				}

			}

			break
		}
	}

	return points, nil
}

func (p *memoryPartition) getSources() []string {
	sources := []string{}

	i := p.sources.NewIterator()
	for i.Next() {
		s, err := i.Value()
		if err == nil {
			sources = append(sources, s.name)
		}
	}

	return sources
}

func (p *memoryPartition) getMetrics(source string) []string {
	metrics := []string{}

	var src *memorySource

	i := p.sources.NewIterator()
	for i.Next() {
		s, err := i.Value()
		if err != nil {
			break
		}

		if s.name == source {
			src = s
		}

		if s.name > source {
			break
		}
	}

	if src == nil {
		return metrics
	}

	metricIterator := src.metrics.NewIterator()
	for metricIterator.Next() {
		m, err := metricIterator.Value()
		if err == nil {
			metrics = append(metrics, m.name)
		}
	}

	return metrics
}

func (p *memoryPartition) close() error {
	p.setReadOnly()

	p.lock.Lock()
	p.sources = nil
	p.lock.Unlock()

	return p.log.close()
}

func (p *memoryPartition) destroy() error {
	err := p.close()
	if err != nil {
		logger.Println(err)
		return err
	}

	return p.log.destroy()
}

// memoryPartition is a partition
var _ partition = &memoryPartition{}
