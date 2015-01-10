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

	for _, src := range p.sources {
		fileSrc := fileSource{
			name: src.name,
		}

		for _, met := range src.metrics {
			logger.Println(src.name,
				met.name)

			gzipWriter.Reset(w)

			fileMet := fileMetric{
				name:      met.name,
				numPoints: len(met.points),
			}

			currentOffset, err := w.Seek(0, 1)
			if err != nil {
				return err
			}

			fileMet.offset = currentOffset

			for _, point := range met.points {
				err = binary.Write(gzipWriter, binary.LittleEndian, point)
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
