package catena

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"os"
	"syscall"
)

var (
	errorInvalidSnapshotMagic = errors.New("catena: invalid snapshot magic sequence")
)

// filePartition represents a catena partition
// stored as a file on disk.
type filePartition struct {
	// Metadata
	minTS int64
	maxTS int64

	// File on disk
	f *os.File

	// Memory mapped backed by f
	mapped []byte

	sources []fileSource
}

// fileSource is a metric source registered on disk.
type fileSource struct {
	name    string
	metrics []fileMetric
}

// fileMetric is a metric on disk.
type fileMetric struct {
	name      string
	offset    int64
	numPoints int
}

// minTimestamp returns the minimum timestamp
// value held by this partition.
func (p *filePartition) minTimestamp() int64 {
	return p.minTS
}

// maxTimestamp returns the maximum timestamp
// value held by this partition.
func (p *filePartition) maxTimestamp() int64 {
	return p.maxTS
}

// readOnly returns whether this partition is read only.
// This is always true for filePartitions.
func (p *filePartition) readOnly() bool {
	return true
}

// addPoint adds a point to the partition and returns
// an error.
func (p *filePartition) addPoint(source, metric string,
	timestamp int64, value float64) error {
	// We can't write to filePartitions.
	return errorReadyOnlyPartition
}

// openFilePartition opens a file with the given file name
// and reads an existing fileParition.
func openFilePartition(filename string) (*filePartition, error) {

	// Open the file.
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	// Run Stat to get the file size.
	fInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}

	mapped, err := syscall.Mmap(int(f.Fd()), 0, int(fInfo.Size()),
		// Set read only protection and shared mapping flag.
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	p := &filePartition{
		f:      f,
		mapped: mapped,
	}

	// Attempt to load the metadata.
	err = p.loadMetadata()
	if err != nil {
		// Failed to read partition metadata, so
		// we need to clean up. There's nothing
		// else we can do.
		munmapErr := syscall.Munmap(mapped)
		if munmapErr != nil {
			logger.Printf("failed to munmap: %v", munmapErr)
		} else {
			p.mapped = nil
		}

		fileCloseErr := f.Close()
		if fileCloseErr != nil {
			logger.Printf("failed to close %s: %v", f.Name(), fileCloseErr)
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

// loadMetadata decodes metadata information for the filePartition from
// the file.
func (p *filePartition) loadMetadata() error {
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

	if magic != snapshotMagic {
		return errorInvalidSnapshotMagic
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
		src := fileSource{}

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
			met := fileMetric{}

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

			// Read number of points. We can't encode/decode
			// an `int` because it does not have a known length.
			numPoints := uint32(0)
			err = binary.Read(r, binary.LittleEndian, &numPoints)
			if err != nil {
				return err
			}

			met.numPoints = int(numPoints)

			src.metrics = append(src.metrics, met)
		}

		p.sources = append(p.sources, src)
	}

	// Internal state has been updated without issues.
	return nil
}

func (p *filePartition) fetchPoints(source, metric string,
	start, end int64) ([]point, error) {

	points := []point{}

	for _, src := range p.sources {
		if src.name == source {
			for _, met := range src.metrics {
				if met.name == metric {
					offset := met.offset

					r := bytes.NewReader(p.mapped)
					_, err := r.Seek(offset, 0)
					if err != nil {
						return nil, err
					}

					gzipReader, err := gzip.NewReader(r)
					if err != nil {
						return nil, err
					}

					for i := 0; i < met.numPoints; i++ {
						pnt := point{}
						err = binary.Read(gzipReader, binary.LittleEndian, &pnt)
						if err != nil {
							return nil, err
						}

						points = append(points, pnt)
					}

					break
				}

				if met.name > metric {
					return nil, errorMetricNotFound
				}
			}

			break
		}

		if src.name > source {
			return nil, errorSourceNotFound
		}
	}

	return points, nil
}

// filePartition is a partition
var _ partition = &filePartition{}
