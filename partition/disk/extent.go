package disk

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"

	"github.com/PreetamJinka/catena"
)

type diskExtent struct {
	startTS   int64
	offset    int64
	numPoints uint32
}

func (p *DiskPartition) extentPoints(e diskExtent) ([]catena.Point, error) {
	r := bytes.NewReader(p.mapped)
	r.Seek(e.offset, 0)

	gzipReader, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}

	points := []catena.Point{}

	for i := uint32(0); i < e.numPoints; i++ {
		point := catena.Point{}

		err = binary.Read(gzipReader, binary.LittleEndian, &point)
		if err != nil {
			return nil, err
		}

		points = append(points, point)
	}

	gzipReader.Close()

	return points, nil
}
