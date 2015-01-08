// Package catena provides a storage engine for
// time series data.
package catena

// DB is a handle to a catena database.
type DB struct {

	// Data directory
	baseDir string

	partitions []partition

	// TODO
}

// NewDB returns a new DB that stores data
// in baseDir.
func NewDB(baseDir string) (*DB, error) {
	db := &DB{
		baseDir: baseDir,
	}

	// TODO

	return db, nil
}
