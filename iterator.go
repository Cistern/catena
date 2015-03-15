package catena

import (
	"errors"

	"github.com/PreetamJinka/catena/partition"
)

// An Iterator is a cursor over an array of points
// for a source and metric.
type Iterator struct {
	source, metric string
	db             *DB
	curPartition   partition.Partition
	partition.Iterator
}

// NewIterator creates a new Iterator for the given source and metric.
func (db *DB) NewIterator(source, metric string) (*Iterator, error) {
	var p partition.Partition

	i := db.partitionList.NewIterator()
	for i.Next() {
		val, _ := i.Value()
		val.Hold()

		if val.HasMetric(source, metric) {
			if p != nil {
				p.Release()
			}

			p = val
		} else {
			val.Release()
		}
	}

	if p == nil {
		return nil, errors.New("catena: couldn't find metric for iterator")
	}

	// We can defer Release because creating a new
	// iterator will hold the partition as well.
	defer p.Release()

	partitionIter, err := p.NewIterator(source, metric)
	if err != nil {
		return nil, err
	}

	return &Iterator{
		source:       source,
		metric:       metric,
		db:           db,
		curPartition: p,
		Iterator:     partitionIter,
	}, nil
}

// Next advances i to the next available point.
func (i *Iterator) Next() error {
	currentPoint := i.Point()
	err := i.Iterator.Next()
	if err == nil {
		return nil
	}

	err = i.Seek(currentPoint.Timestamp + 1)
	return err
}

// Seek moves the iterator to the first timestamp greater than
// or equal to timestamp.
func (i *Iterator) Seek(timestamp int64) error {
	if i.Iterator != nil {
		i.Iterator.Close()
	}

	i.Iterator = nil

	var p partition.Partition

	partitionListIter := i.db.partitionList.NewIterator()
	for partitionListIter.Next() {
		val, _ := partitionListIter.Value()
		val.Hold()

		if val.HasMetric(i.source, i.metric) && (val.MinTimestamp() >= timestamp ||
			(val.MinTimestamp() <= timestamp && val.MaxTimestamp() >= timestamp)) {
			if p != nil {
				p.Release()
			}

			p = val
		} else {
			val.Release()
		}
	}

	if p == nil {
		return errors.New("catena: couldn't find metric for iterator")
	}

	defer p.Release()

	i.curPartition = p

	partitionIter, err := p.NewIterator(i.source, i.metric)
	if err != nil {
		return err
	}

	err = partitionIter.Seek(timestamp)
	if err != nil {
		partitionIter.Close()
		return err
	}

	i.Iterator = partitionIter
	return nil
}

// Reset moves i to the first available timestamp.
func (i *Iterator) Reset() error {
	i.Iterator.Close()

	var p partition.Partition

	partitionListIter := i.db.partitionList.NewIterator()
	for partitionListIter.Next() {
		val, _ := partitionListIter.Value()
		val.Hold()

		if val.HasMetric(i.source, i.metric) {
			if p != nil {
				p.Release()
			}

			p = val
		} else {
			val.Release()
		}
	}

	if p == nil {
		return errors.New("catena: couldn't find metric for iterator")
	}

	defer p.Release()

	i.curPartition = p

	partitionIter, err := p.NewIterator(i.source, i.metric)
	if err != nil {
		return err
	}

	i.Iterator = partitionIter
	return nil
}

// Close closes the iterator. Iterators MUST be closed to unblock
// the compactor!
func (i *Iterator) Close() {
	if i.Iterator == nil {
		return
	}

	i.Iterator.Close()
	i.curPartition = nil
}
