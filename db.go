package catena

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PreetamJinka/catena/partition"
	"github.com/PreetamJinka/catena/partition/disk"
	"github.com/PreetamJinka/catena/partition/memory"
	"github.com/PreetamJinka/catena/wal"
)

// DB is a handle to a catena database.
type DB struct {
	baseDir string

	partitionList *partitionList

	lastPartitionID int64
	partitionSize   int64
	maxPartitions   int

	minTimestamp int64
	maxTimestamp int64

	partitionCreateLock sync.Mutex
}

// NewDB creates a new DB located in baseDir. If baseDir
// does not exist it will be created. An error is returned
// if baseDir is not empty.
func NewDB(baseDir string, partitionSize, maxPartitions int) (*DB, error) {
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

	db := &DB{
		baseDir:       baseDir,
		partitionSize: int64(partitionSize),
		maxPartitions: maxPartitions,
		partitionList: newPartitionList(),
	}

	go func() {
		for _ = range time.Tick(time.Millisecond * 50) {
			db.compact()
		}
	}()

	return db, nil
}

// OpenDB opens a DB located in baseDir.
func OpenDB(baseDir string, partitionSize, maxPartitions int) (*DB, error) {
	db := &DB{
		baseDir:       baseDir,
		partitionSize: int64(partitionSize),
		maxPartitions: maxPartitions,
		partitionList: newPartitionList(),
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

	go func() {
		for _ = range time.Tick(time.Millisecond * 50) {
			db.compact()
		}
	}()

	return db, nil
}

// loadPartitions reads a slice of partition file names
// and updates the internal partition state.
func (db *DB) loadPartitions(names []string) error {

	// Slice of partition IDs
	partitions := []int{}

	isWAL := map[int]bool{}

	for _, name := range names {
		partitionNum := -1

		wal := false

		if strings.HasSuffix(name, ".wal") {
			_, err := fmt.Sscanf(name, "%d.wal", &partitionNum)
			if err != nil {
				return err
			}

			wal = true
		}

		if strings.HasSuffix(name, ".part") {
			_, err := fmt.Sscanf(name, "%d.part", &partitionNum)
			if err != nil {
				return err
			}
		}

		if partitionNum < 0 {
			return errors.New(fmt.Sprintf("catena: invalid partition %s", name))
		}

		if seenWAL, seen := isWAL[partitionNum]; seen {
			if (seenWAL && !wal) || (!seenWAL && wal) {
				// We have both a .wal and a .part, so
				// we'll get rid of the .part and recompact.
				wal = true
				err := os.Remove(filepath.Join(db.baseDir, fmt.Sprintf("%d.part", partitionNum)))
				if err != nil {
					return err
				}
			}
		}

		isWAL[partitionNum] = wal
	}

	for partitionNum := range isWAL {
		partitions = append(partitions, partitionNum)
	}

	// Sort the partitions in increasing order.
	sort.Ints(partitions)

	for _, part := range partitions {
		if int64(part) > db.lastPartitionID {
			db.lastPartitionID = int64(part)
		}

		var p partition.Partition
		var err error
		var filename string

		if isWAL[part] {
			filename = filepath.Join(db.baseDir,
				fmt.Sprintf("%d.wal", part))

			w, err := wal.OpenFileWAL(filename)
			if err != nil {
				return err
			}

			p, err = memory.RecoverMemoryPartition(w)
			if err != nil {
				return err
			}

		} else {
			filename = filepath.Join(db.baseDir,
				fmt.Sprintf("%d.part", part))

			p, err = disk.OpenDiskPartition(filename)
			if err != nil {
				return err
			}
		}

		if db.partitionList.Size() == 1 {
			db.minTimestamp = p.MinTimestamp()
			db.maxTimestamp = p.MaxTimestamp()
		}

		if db.minTimestamp > p.MinTimestamp() {
			db.minTimestamp = p.MinTimestamp()
		}

		if db.maxTimestamp < p.MaxTimestamp() {
			db.maxTimestamp = p.MaxTimestamp()
		}

		db.partitionList.Insert(p)
	}

	return nil
}
