package partition

type Partition interface {
	// Insertion
	InsertRows([]Row) error

	// Metadata
	ReadOnly() bool
	Filename() string
	MinTimestamp() int64
	MaxTimestamp() int64

	// Management
	SetReadOnly()
	Close() error
	Destroy() error

	// Opaque locking
	Hold()
	Release()
	ExclusiveHold()
	ExclusiveRelease()
}
