package catena

type Row struct {
	Source    string
	Metric    string
	Timestamp int64
	Value     float64
}

type Rows []Row
