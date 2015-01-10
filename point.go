package catena

// Point represents an observation
// at a specific timestamp
type Point struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}
