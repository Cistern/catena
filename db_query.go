package catena

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
	Source string
	Metric string
	Start  int64
	End    int64
}

// A QueryResponse is returned after querying
// the DB with a QueryDesc.
type QueryResponse struct {
	Series []Series `json:"series"`
}

// Query fetches series matching the QueryDescs
// and returns a QueryResponse.
func (db *DB) Query(descs []QueryDesc) QueryResponse {
	db.partitionsLock.Lock()
	defer db.partitionsLock.Unlock()

	response := QueryResponse{
		Series: []Series{},
	}

	type sourceMetricKey struct {
		source string
		metric string
	}

	for _, desc := range descs {

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

		for _, part := range partitions {
			logger.Println("querying", part.filename())
			points, err := part.fetchPoints(desc.Source, desc.Metric, desc.Start, desc.End)
			if err != nil {
				logger.Println(err)
				continue
			}

			if len(points) == 0 {
				logger.Println()
				continue
			}

			series.Points = append(series.Points, points...)
		}

		if len(series.Points) == 0 {
			logger.Println()
			continue
		}

		series.Start = series.Points[0].Timestamp
		series.End = series.Points[len(series.Points)-1].Timestamp

		response.Series = append(response.Series, series)
	}

	return response
}
