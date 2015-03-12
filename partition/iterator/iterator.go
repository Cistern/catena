package iterator

import (
	"github.com/PreetamJinka/catena/partition"
)

// Iterator is an iterator over a sequence of points.
type Iterator interface {
	Reset() error
	Next() error
	Point() partition.Point
	Seek(int64) error
	Close()
}
