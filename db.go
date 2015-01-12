package catena

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// InsertRows inserts rows into the database.
func (db *DB) InsertRows(rows Rows) error {
	db.partitionsLock.Lock()
	defer db.partitionsLock.Unlock()

	partitionMults := map[int]partition{}

	maxTSInDB := 0

	for i, part := range db.partitions {
		if i == 0 {
			maxTSInDB = int(part.maxTimestamp())
		}

		key := int(part.maxTimestamp()) / db.partitionModulus
		partitionMults[key] = part

		if int(part.maxTimestamp()) > maxTSInDB {
			maxTSInDB = int(part.maxTimestamp())
		}
	}

	keyToRows := map[int]Rows{}

	for _, row := range rows {
		key := int(row.Timestamp) / db.partitionModulus
		keyToRows[key] = append(keyToRows[key], row)
	}

	for key, _ := range keyToRows {
		if part := partitionMults[key]; part != nil && part.readOnly() {
			return errors.New("catena: insert into read-only partition(s)")
		}
	}

	keys := []int{}
	for key := range keyToRows {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	for _, key := range keys {
		part := partitionMults[key]
		if part == nil {

			// make sure the new keys are strictly
			// larger than the largest timestamp
			for _, row := range keyToRows[key] {
				if int(row.Timestamp) < maxTSInDB && len(db.partitions) > 0 {
					return errors.New("catena: row being inserted is too old")
				}
			}

			// create a new memory partition
			db.lastPartitionID++

			logger.Println("creating new partition with ID", db.lastPartitionID, "for key", key)
			walFileName := filepath.Join(db.baseDir,
				fmt.Sprintf("%d.wal", db.lastPartitionID))
			log, err := newFileWAL(walFileName)

			if err != nil {
				return err
			}

			logger.Println("created new WAL:", walFileName)

			mp, err := newMemoryPartition(log)
			if err != nil {
				log.close()
				return err
			}

			part = mp
			db.partitions = append(db.partitions, part)

			go db.compactPartitions()
		}

		rows := keyToRows[key]
		err := part.put(rows)

		if err != nil {
			return err
		}
	}

	return nil
}

// compactPartitions converts older WAL partitions
// into compressed file partitions.
func (db *DB) compactPartitions() {
	db.partitionsLock.Lock()
	defer db.partitionsLock.Unlock()

	for len(db.partitions) > db.maxPartitions {
		db.partitions[0].destroy()
		db.partitions = db.partitions[1:]
	}

	for i := 0; i < len(db.partitions)-2; i++ {
		part := db.partitions[i]

		if !part.readOnly() {

			logger.Println(part.filename(), "needs to be compacted")

			logger.Println("starting compaction of", part.filename())

			// Compact
			mp := part.(*memoryPartition)

			mp.setReadOnly()

			walName := mp.log.filename()
			partitionName := strings.TrimSuffix(walName, ".wal") + ".part"

			f, err := os.Create(partitionName)
			if err != nil {
				return
			}

			err = mp.serialize(f)
			if err == nil {
				err = mp.destroy()
				if err != nil {
					logger.Println(err)
				}
			}
			f.Sync()
			f.Close()

			logger.Println("compacted to", partitionName)

			fp, err := openFilePartition(partitionName)
			if err != nil {
				return
			}

			logger.Println("loaded partition", partitionName)

			db.partitions[i] = fp

		}
	}
}
