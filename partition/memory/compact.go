package memory

import (
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"sort"
)

var (
	errorPartitionNotReadyOnly = errors.New("partition/memory: partition is not read only")
)

const snapshotMagic = uint32(0xcafec0de)

func (p *MemoryPartition) Compact(w io.WriteSeeker) error {
	p.partitionLock.RLock()
	defer p.partitionLock.RUnlock()

	if !p.readOnly {
		return errorPartitionNotReadyOnly
	}

	type metaKey struct {
		source string
		metric string
	}

	type metaValue struct {
		Offset    int64
		NumPoints uint32
	}

	meta := map[metaKey]metaValue{}

	gzipWriter := gzip.NewWriter(w)

	sources := []string{}
	metricsBySource := map[string][]string{}

	for sourceName, source := range p.sources {
		sources = append(sources, sourceName)

		for metricName, metric := range source.metrics {
			metricsBySource[sourceName] = append(metricsBySource[sourceName], metricName)

			gzipWriter.Reset(w)

			currentOffset, err := w.Seek(0, 1)
			if err != nil {
				return err
			}

			meta[metaKey{sourceName, metricName}] = metaValue{currentOffset, uint32(len(metric.points))}

			err = binary.Write(gzipWriter, binary.LittleEndian, metric.points)
			if err != nil {
				return err
			}

			err = gzipWriter.Flush()
			if err != nil {
				return err
			}
		}
	}

	err := gzipWriter.Close()
	if err != nil {
		return err
	}

	metaStartOffset, err := w.Seek(0, 1)
	if err != nil {
		return err
	}

	// Start writing metadata
	// Magic sequence
	err = binary.Write(w, binary.BigEndian, snapshotMagic)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, p.minTS)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, p.maxTS)
	if err != nil {
		return err
	}

	// Encode the number of sources
	err = binary.Write(w, binary.LittleEndian, uint16(len(sources)))
	if err != nil {
		return err
	}

	sort.Strings(sources)

	for _, sourceName := range sources {
		err = binary.Write(w, binary.LittleEndian, uint8(len(sourceName)))
		if err != nil {
			return err
		}

		_, err = w.Write([]byte(sourceName))
		if err != nil {
			return err
		}

		metrics := metricsBySource[sourceName]
		sort.Strings(metrics)

		// Encode number of metrics
		err = binary.Write(w, binary.LittleEndian, uint16(len(metrics)))
		if err != nil {
			return err
		}

		for _, metricName := range metrics {
			err = binary.Write(w, binary.LittleEndian, uint8(len(metricName)))
			if err != nil {
				return err
			}

			_, err = w.Write([]byte(metricName))
			if err != nil {
				return err
			}

			metadata := meta[metaKey{sourceName, metricName}]

			err = binary.Write(w, binary.LittleEndian, metadata.Offset)
			if err != nil {
				return err
			}

			err = binary.Write(w, binary.LittleEndian, uint32(metadata.NumPoints))
			if err != nil {
				return err
			}
		}
	}

	err = binary.Write(w, binary.LittleEndian, metaStartOffset)
	if err != nil {
		return err
	}

	return nil
}
