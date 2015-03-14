package memory

import (
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"sort"

	"github.com/PreetamJinka/catena/partition"
	"github.com/PreetamJinka/catena/partition/disk"
)

const extentSize = 3600

var (
	errorPartitionNotReadyOnly = errors.New("partition/memory: partition is not read only")
)

type metaKey struct {
	source string
	metric string
}

type extent struct {
	startTS   int64
	offset    int64
	numPoints uint32
	points    []partition.Point
}

type metaValue struct {
	offset    int64
	numPoints uint32

	extents []extent
}

func (p *MemoryPartition) Compact(w io.WriteSeeker) error {
	if !p.readOnly {
		return errorPartitionNotReadyOnly
	}

	meta := map[metaKey]metaValue{}

	gzipWriter := gzip.NewWriter(w)

	sources := []string{}
	metricsBySource := map[string][]string{}

	for sourceName, source := range p.sources {
		sources = append(sources, sourceName)

		for metricName, metric := range source.metrics {
			metricsBySource[sourceName] = append(metricsBySource[sourceName], metricName)

			extents := splitIntoExtents(metric.points)

			currentOffset, err := w.Seek(0, 1)
			if err != nil {
				return err
			}

			metaVal := metaValue{
				offset:    currentOffset,
				numPoints: uint32(len(metric.points)),
			}

			for extentIndex, ext := range extents {
				gzipWriter.Reset(w)
				currentOffset, err := w.Seek(0, 1)
				if err != nil {
					return err
				}

				ext.offset = currentOffset

				err = binary.Write(gzipWriter, binary.LittleEndian, ext.points)
				if err != nil {
					return err
				}

				err = gzipWriter.Flush()
				if err != nil {
					return err
				}

				extents[extentIndex] = ext
			}

			metaVal.extents = extents

			meta[metaKey{sourceName, metricName}] = metaVal
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
	err = binary.Write(w, binary.BigEndian, disk.Magic)
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

			err = binary.Write(w, binary.LittleEndian, metadata.offset)
			if err != nil {
				return err
			}

			err = binary.Write(w, binary.LittleEndian, uint32(metadata.numPoints))
			if err != nil {
				return err
			}

			err = binary.Write(w, binary.LittleEndian, uint32(len(metadata.extents)))
			if err != nil {
				return err
			}

			for _, ext := range metadata.extents {
				err = binary.Write(w, binary.LittleEndian, ext.startTS)
				if err != nil {
					return err
				}

				err = binary.Write(w, binary.LittleEndian, ext.offset)
				if err != nil {
					return err
				}

				err = binary.Write(w, binary.LittleEndian, ext.numPoints)
				if err != nil {
					return err
				}
			}
		}
	}

	err = binary.Write(w, binary.LittleEndian, metaStartOffset)
	if err != nil {
		return err
	}

	return nil
}

func splitIntoExtents(points []partition.Point) []extent {
	extents := []extent{}

	currentExtent := extent{}

	for _, point := range points {
		if currentExtent.numPoints == 0 {
			currentExtent.startTS = point.Timestamp
		}

		currentExtent.points = append(currentExtent.points, point)
		currentExtent.numPoints++

		if currentExtent.numPoints == extentSize {
			extents = append(extents, currentExtent)
			currentExtent = extent{}
		}
	}

	if currentExtent.numPoints > 0 {
		extents = append(extents, currentExtent)
	}

	return extents
}
