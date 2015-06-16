// Package wal provides a write-ahead log.
package wal

import (
	"github.com/Preetam/catena/partition"
)

type walOperation byte

const (
	OperationInsert walOperation = iota
)

// A WAL is a write-ahead log.
type WAL interface {
	Append(WALEntry) (int, error)
	ReadEntry() (WALEntry, error)
	Truncate() error
	Close() error
	Destroy() error
	Filename() string
}

// WALEntry is an entry in the write-ahead log.
type WALEntry struct {
	Operation walOperation
	Rows      []partition.Row
}
