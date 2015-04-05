package catena

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/PreetamJinka/catena/partition"
	"github.com/PreetamJinka/catena/partition/memory"
	"github.com/PreetamJinka/catena/wal"
)

// InsertRows inserts the given rows into the database.
func (db *DB) InsertRows(rows []Row) error {
	keyToRows := map[int][]Row{}

	for _, row := range rows {
		key := int(row.Timestamp / db.partitionSize)
		keyToRows[key] = append(keyToRows[key], row)
	}

	keys := []int{}
	for key := range keyToRows {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	for _, key := range keys {
		rowsForKey := keyToRows[key]

		minTimestampInRows := int64(0)
		maxTimestampInRows := int64(0)

		for i, row := range rowsForKey {
			if i == 1 {
				minTimestampInRows = row.Timestamp
				maxTimestampInRows = row.Timestamp
			}

			if row.Timestamp > maxTimestampInRows {
				maxTimestampInRows = row.Timestamp
			}

			if row.Timestamp < minTimestampInRows {
				minTimestampInRows = row.Timestamp
			}
		}

		var p partition.Partition

	FIND_PARTITION:

		i := db.partitionList.NewIterator()
		for i.Next() {
			val, _ := i.Value()
			val.Hold()

			if val.MinTimestamp()/db.partitionSize == int64(key) {
				p = val
				goto VALID_PARTITION
			}

			if val.MinTimestamp()/db.partitionSize < int64(key) && val.MaxTimestamp()/db.partitionSize >= int64(key) {
				p = val
				goto VALID_PARTITION
			}

			val.Release()
		}

		db.partitionCreateLock.Lock()
		if p == nil {
			if int64(key) < atomic.LoadInt64(&db.minTimestamp)/db.partitionSize {
				db.partitionCreateLock.Unlock()
				return errors.New("catena: row(s) being inserted are too old")
			}

			if db.partitionList.Size() == 0 ||
				int64(key) > atomic.LoadInt64(&db.maxTimestamp)/db.partitionSize {

				// Need to make a new partition
				newPartitionID := atomic.LoadInt64(&db.lastPartitionID) + 1
				w, err := wal.NewFileWAL(filepath.Join(db.baseDir,
					fmt.Sprintf("%d.wal", newPartitionID)))
				if err != nil {
					// Couldn't create the WAL. Maybe another writer has created
					// the WAL file. Retry.
					db.partitionCreateLock.Unlock()
					goto FIND_PARTITION
				}

				p = memory.NewMemoryPartition(w)
				db.partitionList.Insert(p)
				p.Hold()

				if db.partitionList.Size() == 1 {
					atomic.SwapInt64(&db.minTimestamp, minTimestampInRows)
					atomic.SwapInt64(&db.maxTimestamp, maxTimestampInRows)
				}

				if !atomic.CompareAndSwapInt64(&db.lastPartitionID, newPartitionID-1, newPartitionID) {
					p.Release()
					p.Destroy()
					db.partitionCreateLock.Unlock()
					goto FIND_PARTITION
				}
			}

			if p == nil {
				db.partitionCreateLock.Unlock()
				goto FIND_PARTITION
			}
		}

		db.partitionCreateLock.Unlock()

	VALID_PARTITION:

		if p.ReadOnly() {
			p.Release()
			return errors.New("catena: insert into read-only partition")
		}

		err := p.InsertRows(*(*[]partition.Row)(unsafe.Pointer(&rows)))
		if err != nil {
			p.Release()
			return err
		}

		p.Release()

		for min := atomic.LoadInt64(&db.minTimestamp); min > minTimestampInRows; min = atomic.LoadInt64(&db.minTimestamp) {
			if atomic.CompareAndSwapInt64(&db.minTimestamp, min, minTimestampInRows) {
				break
			}
		}

		for max := atomic.LoadInt64(&db.maxTimestamp); max < maxTimestampInRows; max = atomic.LoadInt64(&db.maxTimestamp) {
			if atomic.CompareAndSwapInt64(&db.maxTimestamp, max, maxTimestampInRows) {
				break
			}
		}
	}

	return nil
}
