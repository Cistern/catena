package partition

import (
	"github.com/PreetamJinka/catena"
)

// Iterator is an iterator over a sequence of points.
type Iterator interface {
	Reset() error
	Next() error
	Point() catena.Point
	Seek(int64) error
	Close()
}
