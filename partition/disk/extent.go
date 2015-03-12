package disk

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"

	"github.com/PreetamJinka/catena/partition"
)

type diskExtent struct {
	startTS   int64
	offset    int64
	numPoints uint32
}

func (p *DiskPartition) extentPoints(e diskExtent) ([]partition.Point, error) {
	r := bytes.NewReader(p.mapped)
	r.Seek(e.offset, 0)

	gzipReader, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}

	points := []partition.Point{}

	for i := uint32(0); i < e.numPoints; i++ {
		point := partition.Point{}

		err = binary.Read(gzipReader, binary.LittleEndian, &point)
		if err != nil {
			return nil, err
		}

		points = append(points, point)
	}

	gzipReader.Close()

	return points, nil
}
