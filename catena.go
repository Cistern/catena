// Package catena provides a storage engine for
// time series data.
package catena

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// DB is a handle to a catena database.
type DB struct {

	// Data directory
	baseDir string

	partitionsLock sync.RWMutex
	partitions     []partition

	lastPartitionID  int
	partitionModulus int
}

func NewDB(baseDir string) (*DB, error) {
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		return nil, err
	}

	dir, err := os.Open(baseDir)
	if err != nil {
		return nil, err
	}

	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	if len(names) > 0 {
		return nil, errors.New("catena: NewDB called with non-empty directory")
	}

	return &DB{
		baseDir:          baseDir,
		partitionModulus: 3600,
	}, nil
}

// OpenDB opens a DB located in baseDir.
func OpenDB(baseDir string) (*DB, error) {
	db := &DB{
		baseDir: baseDir,
	}

	dir, err := os.Open(baseDir)
	if err != nil {
		return nil, err
	}

	defer dir.Close()

	dirInfo, err := dir.Stat()
	if err != nil {
		return nil, err
	}

	if !dirInfo.IsDir() {
		return nil, errors.New("catena: baseDir is not a directory")
	}

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	err = db.loadPartitions(names)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) loadPartitions(names []string) error {

	partitions := []int{}

	isWAL := map[int]bool{}

	for _, name := range names {
		if name == logFileName {
			continue
		}

		partitionNum := -1

		if strings.HasSuffix(name, ".wal") {
			_, err := fmt.Sscanf(name, "%d.wal", &partitionNum)
			if err != nil {
				return err
			}

			partitions = append(partitions, partitionNum)
			isWAL[partitionNum] = true

			continue
		}

		_, err := fmt.Sscanf(name, "%d.part", &partitionNum)
		if err != nil {
			return err
		}

		if partitionNum < 0 {
			return errors.New("catena: invalid partition number")
		}

		partitions = append(partitions, partitionNum)
	}

	sort.Ints(partitions)

	for _, part := range partitions {
		if isWAL[part] {
			filename := filepath.Join(db.baseDir,
				fmt.Sprintf("%d.wal", part))

			log, err := openFileWAL(filename)
			if err != nil {
				return err
			}

			mp, err := newMemoryPartition(log)
			if err != nil {
				return err
			}

			db.partitions = append(db.partitions, mp)
			continue
		}
		filename := filepath.Join(db.baseDir,
			fmt.Sprintf("%d.part", part))

		fp, err := openFilePartition(filename)
		if err != nil {
			return err
		}

		db.partitions = append(db.partitions, fp)
	}

	return nil
}
