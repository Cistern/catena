package catena

type walOperation byte

const (
	operationInsert walOperation = iota
)

// A wal is a write-ahead log.
type wal interface {
	append(walEntry) (int, error)
	readEntry() (walEntry, error)
	truncate() error
	close() error
}

// walEntry is an entry in the write-ahead log.
type walEntry struct {
	operation walOperation
	rows      Rows
}
