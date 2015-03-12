package partition

type Point struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type Row struct {
	Source string `json:"source"`
	Metric string `json:"metric"`
	Point
}
