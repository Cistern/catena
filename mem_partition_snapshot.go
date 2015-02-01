package catena

import (
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
)

var (
	errorPartitionNotReadyOnly = errors.New("catena: partition is not read only")
)

const snapshotMagic = uint32(0xcafec0de)

// serialize saves the state of the memory partition and writes to w.
func (p *memoryPartition) serialize(w io.WriteSeeker) error {
	// Make sure p is read only.
	if !p.readOnly() {
		return errorPartitionNotReadyOnly
	}

	filePart := filePartition{
		minTS: p.minTS,
		maxTS: p.maxTS,
	}

	gzipWriter := gzip.NewWriter(w)

	sourceIter := p.sources.NewIterator()

	for sourceIter.Next() {
		src, err := sourceIter.Value()
		if err != nil {
			return err
		}

		fileSrc := fileSource{
			name: src.name,
		}

		metricIter := src.metrics.NewIterator()
		for metricIter.Next() {
			met, err := metricIter.Value()
			if err != nil {
				return err
			}

			gzipWriter.Reset(w)

			fileMet := fileMetric{
				name:      met.name,
				numPoints: met.points.Size(),
			}

			currentOffset, err := w.Seek(0, 1)
			if err != nil {
				return err
			}

			fileMet.offset = currentOffset

			pointIter := met.points.NewIterator()
			for pointIter.Next() {
				point, err := pointIter.Value()
				if err != nil {
					return err
				}

				err = binary.Write(gzipWriter, binary.LittleEndian, point)
				if err != nil {
					return err
				}
			}

			err = gzipWriter.Flush()
			if err != nil {
				return err
			}

			fileSrc.metrics = append(fileSrc.metrics, fileMet)
		}

		filePart.sources = append(filePart.sources, fileSrc)
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

	err = binary.Write(w, binary.LittleEndian, filePart.minTS)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, filePart.maxTS)
	if err != nil {
		return err
	}

	// Encode the number of sources
	err = binary.Write(w, binary.LittleEndian, uint16(len(filePart.sources)))
	if err != nil {
		return err
	}

	for _, src := range filePart.sources {
		err = binary.Write(w, binary.LittleEndian, uint8(len(src.name)))
		if err != nil {
			return err
		}

		_, err = w.Write([]byte(src.name))
		if err != nil {
			return err
		}

		// Encode number of metrics
		err = binary.Write(w, binary.LittleEndian, uint16(len(src.metrics)))
		if err != nil {
			return err
		}

		for _, met := range src.metrics {
			err = binary.Write(w, binary.LittleEndian, uint8(len(met.name)))
			if err != nil {
				return err
			}

			_, err = w.Write([]byte(met.name))
			if err != nil {
				return err
			}

			err = binary.Write(w, binary.LittleEndian, met.offset)
			if err != nil {
				return err
			}

			err = binary.Write(w, binary.LittleEndian, uint32(met.numPoints))
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
