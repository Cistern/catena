package partition

type Partition interface {
	// Insertion
	InsertRows([]Row) error

	// Metadata
	ReadOnly() bool
	Filename() string
	MinTimestamp() int64
	MaxTimestamp() int64

	// Metrics metadata
	Sources() []string
	Metrics(source string) []string
	HasSource(source string) bool
	HasMetric(source, metric string) bool

	// Management
	SetReadOnly()
	Close() error
	Destroy() error

	// Opaque locking
	Hold()
	Release()
	ExclusiveHold()
	ExclusiveRelease()

	// Iterator
	NewIterator(source string, metric string) (Iterator, error)
}

// Iterator is an iterator over a sequence of points.
type Iterator interface {
	Reset() error
	Next() error
	Point() Point
	Seek(int64) error
	Close()
}
