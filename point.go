package catena

import (
	"fmt"
)

// point represents an observation
// at a specific timestamp
type point struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// String returns the string representation
// of the point.
func (p point) String() string {
	return fmt.Sprintf("@%d - %.3e", p.Timestamp, p.Value)
}
