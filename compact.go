package catena

import (
	"os"
	"strings"
	"sync/atomic"

	"github.com/Cistern/catena/partition"
	"github.com/Cistern/catena/partition/disk"
	"github.com/Cistern/catena/partition/memory"
)

// compact drops old partitions and compacts older memory to
// read-only disk partitions.
func (db *DB) compact() {

	// Look for partitions to drop
	i := db.partitionList.NewIterator()
	seen := 0
	lastMin := int64(0)
	for i.Next() {
		p, err := i.Value()
		if err != nil {
			break
		}

		seen++
		if seen <= db.maxPartitions {
			lastMin = p.MinTimestamp()
			continue
		}

		atomic.SwapInt64(&db.minTimestamp, lastMin)

		// Remove it from the list
		db.partitionList.Remove(p)

		// Make sure we're the only ones accessing the partition
		p.ExclusiveHold()
		p.Destroy()
		p.ExclusiveRelease()
	}

	// Find partitions to compact
	toCompact := []partition.Partition{}

	seen = 0
	i = db.partitionList.NewIterator()
	for i.Next() {
		seen++
		if seen <= 2 {
			// Skip the latest two in-memory partitions
			continue
		}

		p, _ := i.Value()

		p.Hold()
		if !p.ReadOnly() {
			p.Release()
			p.ExclusiveHold()
			p.SetReadOnly()
			p.ExclusiveRelease()

			toCompact = append(toCompact, p)
		} else {
			p.Release()
		}
	}

	for _, p := range toCompact {
		// p is read-only, so no need to lock.
		memPart := p.(*memory.MemoryPartition)

		// Create the disk partition file
		filename := strings.TrimSuffix(memPart.Filename(), ".wal") + ".part"
		f, err := os.Create(filename)
		if err != nil {
			// ???
			return
		}

		// Compact
		err = memPart.Compact(f)
		if err != nil {
			// ???
			return
		}

		// Close and reopen.
		f.Sync()
		f.Close()

		diskPart, err := disk.OpenDiskPartition(filename)
		if err != nil {
			// ???
			return
		}

		// Swap the memory partition with the disk partition.
		db.partitionList.Swap(memPart, diskPart)

		memPart.ExclusiveHold()
		memPart.Destroy()
		memPart.ExclusiveRelease()
	}
}
