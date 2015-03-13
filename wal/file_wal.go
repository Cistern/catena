package wal

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math"
	"os"
	"sync"

	"github.com/PreetamJinka/catena/partition"
)

var (
	// Magic sequence to check for valid data.
	walMagic = uint32(0x11141993)

	errorInvalidWALMagic = errors.New("wal: invalid WAL magic number")
	errorInvalidWALFile  = errors.New("wal: invalid WAL file")
)

// A FileWAL is a write-ahead log represented by a file on disk.
type FileWAL struct {
	f    *os.File
	lock sync.Mutex

	filename string

	// lastReadOffset stores end of the last good
	// WAL entry. This way we can truncate the WAL
	// and keep appending valid data at the end.
	lastReadOffset int64
}

// NewFileWAL returns a new on-disk write-ahead log
// with the given file name.
func NewFileWAL(filename string) (*FileWAL, error) {
	// Attempt to open WAL file.
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666)
	if err != nil {
		return nil, err
	}

	return &FileWAL{
		f:        f,
		filename: filename,
	}, nil
}

// OpenFileWAL opens a write-ahead long stored at filename.
func OpenFileWAL(filename string) (*FileWAL, error) {
	// Attempt to open an existing WAL file.
	f, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &FileWAL{
		f:        f,
		filename: filename,
	}, nil
}

// Append writes the WALentry to the write-ahead log.
// It returns the number of bytes written and an error.
func (w *FileWAL) Append(entry WALEntry) (int, error) {

	// Make sure we have an open WAL.
	if w.f == nil {
		return 0, errorInvalidWALFile
	}

	// Buffer writes until the end.
	buf := &bytes.Buffer{}

	var err error

	scratch := [512]byte{}

	// Write magic number
	scratch[0] = byte(walMagic)
	scratch[1] = byte(walMagic >> 8)
	scratch[2] = byte(walMagic >> 16)
	scratch[3] = byte(walMagic >> 24)

	_, err = buf.Write(scratch[:4])
	if err != nil {
		return 0, err
	}

	// Write the operation type
	err = buf.WriteByte(byte(entry.Operation))
	if err != nil {
		return 0, err
	}

	// Write the number of rows
	numRows := uint32(len(entry.Rows))
	scratch[0] = byte(numRows)
	scratch[1] = byte(numRows >> 8)
	scratch[2] = byte(numRows >> 16)
	scratch[3] = byte(numRows >> 24)

	_, err = buf.Write(scratch[:4])
	if err != nil {
		return 0, err
	}

	// Write the size of the entry (0 for now)
	_, err = buf.Write(scratch[4:8])
	if err != nil {
		return 0, err
	}

	gzipWriter, err := gzip.NewWriterLevel(buf, gzip.NoCompression)
	if err != nil {
		return 0, err
	}

	for _, row := range entry.Rows {
		// Write source name length
		_, err = gzipWriter.Write([]byte{byte(len(row.Source)), byte(len(row.Metric))})
		if err != nil {
			return 0, err
		}

		// Write source and metric names
		_, err = gzipWriter.Write([]byte(row.Source + row.Metric))
		if err != nil {
			return 0, err
		}

		// Write timestamp and value
		scratch[0] = byte(row.Point.Timestamp)
		scratch[1] = byte(row.Point.Timestamp >> (8 * 1))
		scratch[2] = byte(row.Point.Timestamp >> (8 * 2))
		scratch[3] = byte(row.Point.Timestamp >> (8 * 3))
		scratch[4] = byte(row.Point.Timestamp >> (8 * 4))
		scratch[5] = byte(row.Point.Timestamp >> (8 * 5))
		scratch[6] = byte(row.Point.Timestamp >> (8 * 6))
		scratch[7] = byte(row.Point.Timestamp >> (8 * 7))
		_, err = gzipWriter.Write(scratch[:8])
		if err != nil {
			return 0, err
		}

		valueBits := math.Float64bits(row.Point.Value)
		scratch[0] = byte(valueBits)
		scratch[1] = byte(valueBits >> (8 * 1))
		scratch[2] = byte(valueBits >> (8 * 2))
		scratch[3] = byte(valueBits >> (8 * 3))
		scratch[4] = byte(valueBits >> (8 * 4))
		scratch[5] = byte(valueBits >> (8 * 5))
		scratch[6] = byte(valueBits >> (8 * 6))
		scratch[7] = byte(valueBits >> (8 * 7))
		_, err = gzipWriter.Write(scratch[:8])
		if err != nil {
			return 0, err
		}
	}

	err = gzipWriter.Close()
	if err != nil {
		return 0, err
	}

	entrySize := buf.Len() - 13

	result := buf.Bytes()

	// Write the size of the entry
	entrySizeUint32 := uint32(entrySize)
	scratch[0] = byte(entrySizeUint32)
	scratch[1] = byte(entrySizeUint32 >> 8)
	scratch[2] = byte(entrySizeUint32 >> 16)
	scratch[3] = byte(entrySizeUint32 >> 24)

	copy(result[9:13], scratch[:4])

	w.lock.Lock()
	// Record the current offset so we can truncate
	// later in case something goes wrong.
	currentOffset, err := w.f.Seek(0, 1)
	if err != nil {
		w.lock.Unlock()
		return 0, err
	}

	w.lastReadOffset = currentOffset

	// Flush to the file.
	n, err := w.f.Write(result)
	if err != nil {
		w.Truncate()
		w.lock.Unlock()
		return 0, err
	}

	w.lock.Unlock()
	return n, err
}

// ReadEntry reads a WALEntry from the write-ahead log.
// If a non-nil error is returned, w.Truncate() may
// be called to make the WAL safe for writing.
func (w *FileWAL) ReadEntry() (WALEntry, error) {
	w.lock.Lock()

	entry := WALEntry{}
	var err error

	// Make sure we have an open WAL.
	if w.f == nil {
		w.lock.Unlock()
		return entry, errorInvalidWALFile
	}

	// Read magic value.
	magic := uint32(0)
	err = binary.Read(w.f, binary.LittleEndian, &magic)
	if err != nil {
		w.lock.Unlock()
		return entry, err
	}

	if magic != walMagic {
		w.lock.Unlock()
		return entry, errorInvalidWALMagic
	}

	// Read the operation type.
	err = binary.Read(w.f, binary.LittleEndian, &entry.Operation)
	if err != nil {
		w.lock.Unlock()
		return entry, err
	}

	// Read the number of rows.
	numRows := uint32(0)
	err = binary.Read(w.f, binary.LittleEndian, &numRows)
	if err != nil {
		w.lock.Unlock()
		return entry, err
	}

	// Read the size of the entry.
	entrySize := uint32(0)
	err = binary.Read(w.f, binary.LittleEndian, &entrySize)
	if err != nil {
		w.lock.Unlock()
		return entry, err
	}

	entryBytes := make([]byte, int(entrySize))
	n, err := w.f.Read(entryBytes)
	if err != nil {
		w.lock.Unlock()
		return entry, err
	}

	if n != int(entrySize) {
		w.lock.Unlock()
		return entry, errors.New("wal: did not read full entry")
	}

	r := bytes.NewReader(entryBytes)
	if err != nil {
		w.lock.Unlock()
		return entry, err
	}

	gzipReader, err := gzip.NewReader(r)
	if err != nil {
		w.lock.Unlock()
		return entry, err
	}
	gzipReader.Close()

	uncompressed, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		w.lock.Unlock()
		return entry, err
	}

	r = bytes.NewReader(uncompressed)

	for i := uint32(0); i < numRows; i++ {
		row := partition.Row{}

		sourceNameLength, metricNameLength := uint8(0), uint8(0)

		// Read the source and metric name lengths.
		err = binary.Read(r, binary.LittleEndian, &sourceNameLength)
		if err != nil {
			w.lock.Unlock()
			return entry, err
		}
		err = binary.Read(r, binary.LittleEndian, &metricNameLength)
		if err != nil {
			w.lock.Unlock()
			return entry, err
		}

		sourceAndMetricNames := make([]byte, int(sourceNameLength+metricNameLength))

		_, err = r.Read(sourceAndMetricNames)
		if err != nil {
			w.lock.Unlock()
			return entry, err
		}

		row.Source = string(sourceAndMetricNames[:int(sourceNameLength)])
		row.Metric = string(sourceAndMetricNames[int(sourceNameLength):])

		err = binary.Read(r, binary.LittleEndian, &row.Point)
		if err != nil {
			w.lock.Unlock()
			return entry, err
		}

		entry.Rows = append(entry.Rows, row)
	}

	// We've decoded everything fine.
	// We now update lastReadOffset to the current offset
	// in the file.
	currentOffset, err := w.f.Seek(0, 1)
	if err != nil {
		w.lock.Unlock()
		return entry, err
	}

	w.lastReadOffset = currentOffset

	w.lock.Unlock()
	return entry, err
}

// Truncate truncates w's backing file to
// lastReadOffset. Truncation ensures that
// new entries can be safely read after
// they are appended.
func (w *FileWAL) Truncate() error {
	return w.f.Truncate(w.lastReadOffset)
}

// Close flushes any pending writes and closes the file.
func (w *FileWAL) Close() error {
	w.f.Sync()
	w.f.Close()
	return nil
}

// Destroy closes the FileWAL and removes the
// file on disk.
func (w *FileWAL) Destroy() error {
	w.Close()
	err := os.Remove(w.filename)
	return err
}
func (w *FileWAL) Filename() string {
	return w.filename
}

// FileWAL is a WAL
var _ WAL = &FileWAL{}
