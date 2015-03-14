package catena

import (
	"os"
	"strings"

	"github.com/PreetamJinka/catena/partition"
	"github.com/PreetamJinka/catena/partition/disk"
	"github.com/PreetamJinka/catena/partition/memory"
)

func (db *DB) compact() {

	i := db.partitionList.NewIterator()
	seen := 0
	for i.Next() {
		seen++
		if seen <= db.maxPartitions {
			continue
		}

		p, err := i.Value()
		if err != nil {
			break
		}
		db.partitionList.Remove(p)
		p.ExclusiveHold()
		p.Destroy()
		p.ExclusiveRelease()
	}

	toCompact := []partition.Partition{}

	seen = 0
	i = db.partitionList.NewIterator()
	for i.Next() {
		seen++
		if seen <= 2 {
			continue
		}

		p, _ := i.Value()

		if !p.ReadOnly() {
			p.ExclusiveHold()
			p.SetReadOnly()
			p.ExclusiveRelease()

			toCompact = append(toCompact, p)
		}
	}

	for _, p := range toCompact {
		memPart := p.(*memory.MemoryPartition)
		filename := strings.TrimSuffix(memPart.Filename(), ".wal") + ".part"
		f, err := os.Create(filename)
		if err != nil {
			// ???
			return
		}

		memPart.Compact(f)
		if err != nil {
			// ???
			return
		}

		f.Sync()
		f.Close()

		diskPart, err := disk.OpenDiskPartition(filename)
		if err != nil {
			// ???
			return
		}

		db.partitionList.Swap(memPart, diskPart)

		memPart.Destroy()
	}
}
