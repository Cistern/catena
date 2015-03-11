package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"syscall"
)

const Magic = uint32(0xcafec0de)

// diskPartition represents a partition
// stored as a file on disk.
type DiskPartition struct {
	// Metadata
	minTS int64
	maxTS int64

	// File on disk
	f        *os.File
	filename string

	// Memory mapped backed by f
	mapped []byte

	sources map[string]diskSource
}

// diskSource is a metric source registered on disk.
type diskSource struct {
	name    string
	metrics map[string]diskMetric
}

// diskMetric is a metric on disk.
type diskMetric struct {
	name      string
	offset    int64
	numPoints uint32

	extents []diskExtent
}

func OpenDiskPartition(filename string) (*DiskPartition, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	// Run Stat to get the file size.
	fInfo, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	mapped, err := syscall.Mmap(int(f.Fd()), 0, int(fInfo.Size()),
		// Set read only protection and shared mapping flag.
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}

	p := &DiskPartition{
		f:        f,
		filename: filename,
		mapped:   mapped,
		sources:  map[string]diskSource{},
	}

	// Attempt to load the metadata.
	err = p.readMetadata()
	if err != nil {
		// Failed to read partition metadata, so
		// we need to clean up. There's nothing
		// else we can do.
		munmapErr := syscall.Munmap(mapped)
		if munmapErr != nil {
			// What do we do?
		} else {
			p.mapped = nil
		}

		fileCloseErr := f.Close()
		if fileCloseErr != nil {
			// What do we do?
		} else {
			p.f = nil
		}

		p.minTS = 0
		p.maxTS = 0
		p.sources = nil

		return nil, err
	}

	// Everything went well.
	return p, nil
}

// readMetadata decodes metadata information for the DiskPartition from
// the file.
func (p *DiskPartition) readMetadata() error {
	r := bytes.NewReader(p.mapped)

	// Read metadata offset.
	_, err := r.Seek(-8, 2)
	if err != nil {
		return err
	}

	metaStartOffset := int64(0)
	err = binary.Read(r, binary.LittleEndian, &metaStartOffset)
	if err != nil {
		return err
	}

	// Seek to the start of the metadata offset.
	_, err = r.Seek(metaStartOffset, 0)
	if err != nil {
		return err
	}

	// Check magic sequence number
	magic := uint32(0)
	err = binary.Read(r, binary.BigEndian, &magic)
	if err != nil {
		return err
	}

	if magic != Magic {
		return errors.New("partition/disk: invalid magic")
	}

	// Read min and max timestamps.
	err = binary.Read(r, binary.LittleEndian, &p.minTS)
	if err != nil {
		return err
	}

	err = binary.Read(r, binary.LittleEndian, &p.maxTS)
	if err != nil {
		return err
	}

	// Read the number of sources.
	numSources := uint16(0)
	err = binary.Read(r, binary.LittleEndian, &numSources)
	if err != nil {
		return err
	}

	// Read each source.
	for i := uint16(0); i < numSources; i++ {
		src := diskSource{
			metrics: map[string]diskMetric{},
		}

		// Read the length of the name.
		srcNameLength := uint8(0)
		err = binary.Read(r, binary.LittleEndian, &srcNameLength)
		if err != nil {
			return err
		}

		// Read the name.
		srcNameBytes := make([]byte, int(srcNameLength))
		_, err = r.Read(srcNameBytes)
		if err != nil {
			return err
		}

		src.name = string(srcNameBytes)

		// Read the number of metrics for this source.
		numMetrics := uint16(0)
		err = binary.Read(r, binary.LittleEndian, &numMetrics)
		if err != nil {
			return err
		}

		// Read each meric.
		for j := uint16(0); j < numMetrics; j++ {
			met := diskMetric{}

			// Read the length of the name.
			metNameLength := uint8(0)
			err = binary.Read(r, binary.LittleEndian, &metNameLength)
			if err != nil {
				return err
			}

			// Read the name.
			metNameBytes := make([]byte, int(metNameLength))
			_, err = r.Read(metNameBytes)
			if err != nil {
				return err
			}

			met.name = string(metNameBytes)

			// Read metric offset.
			err = binary.Read(r, binary.LittleEndian, &met.offset)
			if err != nil {
				return err
			}

			// Read number of points.
			err = binary.Read(r, binary.LittleEndian, &met.numPoints)
			if err != nil {
				return err
			}

			// Read number of extents.
			numExtents := uint32(0)
			err = binary.Read(r, binary.LittleEndian, &numExtents)
			if err != nil {
				return err
			}

			for i := uint32(0); i < numExtents; i++ {
				ext := diskExtent{}
				err = binary.Read(r, binary.LittleEndian, &ext.startTS)
				if err != nil {
					return err
				}

				err = binary.Read(r, binary.LittleEndian, &ext.offset)
				if err != nil {
					return err
				}

				err = binary.Read(r, binary.LittleEndian, &ext.numPoints)
				if err != nil {
					return err
				}

				met.extents = append(met.extents, ext)
			}

			src.metrics[met.name] = met
		}

		p.sources[src.name] = src
	}

	// Internal state has been updated without issues.
	return nil
}

func (p *DiskPartition) Close() error {
	err := syscall.Munmap(p.mapped)
	if err != nil {
		return err
	}

	p.mapped = nil
	return p.f.Close()
}

func (p *DiskPartition) Destroy() error {
	err := p.Close()
	if err != nil {
		return err
	}

	return os.Remove(p.filename)
}
