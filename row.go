package catena

type Row struct {
	Source    string  `json:"source"`
	Metric    string  `json:"metric"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type Rows []Row
