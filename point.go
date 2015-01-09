package catena

// point represents an observation
// at a specific timestamp
type point struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}
