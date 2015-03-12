package partition

type Partition interface {
	InsertRows([]Row) error
	SetReadOnly()
	ReadOnly() bool
	MinTimestamp() int64
	MaxTimestamp() int64
	Close() error
	Destroy() error
}
