package catena

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

var (
	// Magic sequence to check for valid data.
	walMagic = uint32(0x11141993)

	errorInvalidWALMagic = errors.New("catena: invalid WAL magic number")
	errorInvalidWALFile  = errors.New("catena: invalid WAL file")
)

// fileWAL is a write-ahead backed by a file on disk.
type fileWAL struct {

	// File used by the WAL.
	f *os.File

	// lastReadOffset stores end of the last good
	// WAL entry. This way we can truncate the WAL
	// and keep appending valid data at the end.
	lastReadOffset int64
}

// newFileWAL returns a new on-disk write-ahead log
// with the given file name.
func newFileWAL(filename string) (*fileWAL, error) {
	// Attempt to open WAL file.
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return &fileWAL{
		f: f,
	}, nil
}

// openFileWAL opens a write-ahead long stored at filename.
func openFileWAL(filename string) (*fileWAL, error) {
	// Attempt to open an existing WAL file.
	f, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &fileWAL{
		f: f,
	}, nil
}

// append writes the walEntry to the write-ahead log.
// It returns the number of bytes written and an error.
func (w *fileWAL) append(entry walEntry) (int, error) {

	// Make sure we have an open WAL.
	if w.f == nil {
		return 0, errorInvalidWALFile
	}

	// Buffer writes until the end.
	buf := &bytes.Buffer{}

	var err error

	// Write magic number
	err = binary.Write(buf, binary.LittleEndian, walMagic)
	if err != nil {
		return 0, err
	}

	// Write the operation type
	err = binary.Write(buf, binary.LittleEndian, entry.operation)
	if err != nil {
		return 0, err
	}

	// Write the number of rows
	err = binary.Write(buf, binary.LittleEndian, uint16(len(entry.rows)))
	if err != nil {
		return 0, err
	}

	for _, row := range entry.rows {
		// Write source name length
		err = binary.Write(buf, binary.LittleEndian, uint8(len(row.Source)))
		if err != nil {
			return 0, err
		}

		// Write metric name length
		err = binary.Write(buf, binary.LittleEndian, uint8(len(row.Metric)))
		if err != nil {
			return 0, err
		}

		// Write source and metric names
		_, err = buf.WriteString(row.Source + row.Metric)
		if err != nil {
			return 0, err
		}

		// Write timestamp and value
		err = binary.Write(buf, binary.LittleEndian, point{
			Timestamp: row.Timestamp,
			Value:     row.Value,
		})
		if err != nil {
			return 0, err
		}
	}

	// Flush to the file.
	n, err := w.f.Write(buf.Bytes())
	if err != nil {
		return n, err
	}

	// Sync for durability.
	err = w.f.Sync()

	return n, err
}

// readEntry reads a walEntry from the write-ahead log.
// If a non-nil error is returned, w.truncate() may
// be called to make the WAL safe for writing.
func (w *fileWAL) readEntry() (walEntry, error) {
	entry := walEntry{}
	var err error

	// Make sure we have an open WAL.
	if w.f == nil {
		return entry, errorInvalidWALFile
	}

	// Read magic value.
	magic := uint32(0)
	err = binary.Read(w.f, binary.LittleEndian, &magic)
	if err != nil {
		return entry, err
	}

	if magic != walMagic {
		return entry, errorInvalidWALMagic
	}

	// Read the operation type.
	err = binary.Read(w.f, binary.LittleEndian, &entry.operation)
	if err != nil {
		return entry, err
	}

	// Read the number of rows.
	numRows := uint16(0)
	err = binary.Read(w.f, binary.LittleEndian, &numRows)
	if err != nil {
		return entry, err
	}

	for i := uint16(0); i < numRows; i++ {
		row := walRow{}

		sourceNameLength, metricNameLength := uint8(0), uint8(0)

		// Read the source and metric name lengths.
		err = binary.Read(w.f, binary.LittleEndian, &sourceNameLength)
		if err != nil {
			return entry, err
		}
		err = binary.Read(w.f, binary.LittleEndian, &metricNameLength)
		if err != nil {
			return entry, err
		}

		sourceAndMetricNames := make([]byte, int(sourceNameLength+metricNameLength))

		_, err = w.f.Read(sourceAndMetricNames)
		if err != nil {
			return entry, err
		}

		row.Source = string(sourceAndMetricNames[:int(sourceNameLength)])
		row.Metric = string(sourceAndMetricNames[int(sourceNameLength):])

		tmpPoint := point{}
		err = binary.Read(w.f, binary.LittleEndian, &tmpPoint)
		if err != nil {
			return entry, err
		}

		row.Timestamp = tmpPoint.Timestamp
		row.Value = tmpPoint.Value

		entry.rows = append(entry.rows, row)
	}

	// We've decoded everything fine.
	// We now update lastReadOffset to the current offset
	// in the file.
	currentOffset, err := w.f.Seek(0, 1)
	if err != nil {
		return entry, err
	}

	w.lastReadOffset = currentOffset

	return entry, err
}

// truncate truncates w's backing file to
// lastReadOffset. Truncation ensures that
// new entries can be safely read after
// they are appended.
func (w *fileWAL) truncate() error {
	return w.f.Truncate(w.lastReadOffset)
}

// close closes the file used by w.
func (w *fileWAL) close() error {
	err := w.f.Close()
	if err != nil {
		return err
	}

	w.f = nil
	w.lastReadOffset = 0
	return nil
}

// fileWAL is a wal.
var _ wal = &fileWAL{}
