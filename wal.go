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

// walRow represents a single point or observation
// of a time series.
type walRow struct {
	Source, Metric string
	Timestamp      int64
	Value          float64
}

// walEntry is an entry in the write-ahead log.
type walEntry struct {
	operation walOperation
	rows      []walRow
}
