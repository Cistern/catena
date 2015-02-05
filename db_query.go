package catena

import (
	"sync"
)

// A Series is an ordered set of points
// for a source and metric over a range
// of time.
type Series struct {
	// First timestamp
	Start int64 `json:"start"`

	// Last timestamp
	End int64 `json:"end"`

	Source string `json:"source"`
	Metric string `json:"metric"`

	Points []Point `json:"points"`
}

// A QueryDesc is a description of a
// query. It specifies a source, metric,
// start, and end timestamps.
type QueryDesc struct {
	Source string `json:"source"`
	Metric string `json:"metric"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
}

// A QueryResponse is returned after querying
// the DB with a QueryDesc.
type QueryResponse struct {
	Series []Series `json:"series"`
}

// Query fetches series matching the QueryDescs
// and returns a QueryResponse.
func (db *DB) Query(descs []QueryDesc) QueryResponse {
	db.partitionsLock.RLock()
	defer db.partitionsLock.RUnlock()

	response := QueryResponse{
		Series: []Series{},
	}

	type sourceMetricKey struct {
		source string
		metric string
	}

	seriesChan := make(chan Series, len(descs))
	seriesWg := sync.WaitGroup{}
	seriesWg.Add(len(descs))

	go func() {
		seriesWg.Wait()
		close(seriesChan)
	}()

	for _, d := range descs {
		go func(desc QueryDesc) {
			defer seriesWg.Done()

			// Get list of partitions to query
			partitions := []partition{}

			for _, part := range db.partitions {
				if part.minTimestamp() >= desc.Start {
					if part.minTimestamp() <= desc.End {
						partitions = append(partitions, part)
						continue
					}
				} else {
					if part.maxTimestamp() >= desc.Start {
						partitions = append(partitions, part)
						continue
					}
				}
			}

			logger.Println("partitions to query:")
			for _, part := range partitions {
				logger.Println("  ", part.filename(),
					"with ranges\n", "    ",
					part.minTimestamp(), "-", part.maxTimestamp())
			}

			series := Series{
				Source: desc.Source,
				Metric: desc.Metric,
			}

			pointsChan := make(chan []Point)
			pointsWg := sync.WaitGroup{}
			pointsWg.Add(len(partitions))

			go func() {
				pointsWg.Wait()
				close(pointsChan)
			}()

			for _, p := range partitions {
				go func(part partition) {
					defer pointsWg.Done()

					logger.Println("querying", part.filename())
					points, err := part.fetchPoints(desc.Source, desc.Metric, desc.Start, desc.End)
					if err != nil {
						logger.Println(err)
						return
					}

					if len(points) == 0 {
						logger.Println()
						return
					}

					pointsChan <- points
				}(p)
			}

			for points := range pointsChan {
				series.Points = append(series.Points, points...)
			}

			if len(series.Points) == 0 {
				return
			}

			series.Start = series.Points[0].Timestamp
			series.End = series.Points[len(series.Points)-1].Timestamp

			seriesChan <- series
		}(d)
	}

	for series := range seriesChan {
		response.Series = append(response.Series, series)
	}

	return response
}

// Sources returns a list of sources present during the given interval.
func (db *DB) Sources(start, end int64) []string {
	db.partitionsLock.RLock()
	defer db.partitionsLock.RUnlock()

	// Get list of partitions to query
	partitions := []partition{}

	for _, part := range db.partitions {
		if part.minTimestamp() >= start {
			if part.minTimestamp() <= end {
				partitions = append(partitions, part)
				continue
			}
		} else {
			if part.maxTimestamp() >= start {
				partitions = append(partitions, part)
				continue
			}
		}
	}

	sources := []string{}
	sourcesMap := map[string]bool{}

	for _, part := range partitions {
		for _, src := range part.getSources() {
			sourcesMap[src] = true
		}
	}

	for src := range sourcesMap {
		sources = append(sources, src)
	}

	return sources
}

// Metrics returns a list of metrics present for the given source during the given interval.
func (db *DB) Metrics(source string, start, end int64) []string {
	db.partitionsLock.RLock()
	defer db.partitionsLock.RUnlock()

	// Get list of partitions to query
	partitions := []partition{}

	for _, part := range db.partitions {
		if part.minTimestamp() >= start {
			if part.minTimestamp() <= end {
				partitions = append(partitions, part)
				continue
			}
		} else {
			if part.maxTimestamp() >= start {
				partitions = append(partitions, part)
				continue
			}
		}
	}

	metrics := []string{}
	metricsMap := map[string]bool{}

	for _, part := range partitions {
		for _, met := range part.getMetrics(source) {
			metricsMap[met] = true
		}
	}

	for met := range metricsMap {
		metrics = append(metrics, met)
	}

	return metrics
}
