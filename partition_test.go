package catena

import (
	"reflect"
	"testing"
	"time"
)

func TestMemoryPartition(t *testing.T) {
	// We must open a WAL.
	log, err := newFileWAL("/tmp/TestMemoryPartition.wal")
	if err != nil {
		t.Fatal(err)
	}

	p, err := newMemoryPartition(log)
	if err != nil {
		t.Fatal(err)
	}

	err = p.put(Rows{
		Row{
			Source:    "hostA",
			Metric:    "metric.1",
			Timestamp: 123,
			Value:     0.234,
		},
		Row{
			Source:    "hostB",
			Metric:    "metric.1",
			Timestamp: 123,
			Value:     0.234,
		},
		Row{
			Source:    "hostA",
			Metric:    "metric.2",
			Timestamp: 123,
			Value:     0.234,
		},
		Row{
			Source:    "hostC",
			Metric:    "metric.1",
			Timestamp: 123,
			Value:     -123,
		},
		Row{
			Source:    "hostA",
			Metric:    "metric.1",
			Timestamp: -1,
			Value:     0.234,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for a very short amount of time
	// for the in-memory structure to update.
	time.Sleep(20 * time.Nanosecond)

	expectedPoints := []point{
		point{
			Timestamp: 123,
			Value:     0.234,
		},
	}

	points, err := p.fetchPoints("hostA", "metric.1", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(points, expectedPoints) {
		t.Errorf("expected points %v, got %v", expectedPoints, points)
	}
}

func TestMemoryPartitionRecover(t *testing.T) {
	// Open a new test WAL file. We truncate any existing file.
	w, err := newFileWAL("/tmp/TestMemoryPartitionRecover.wal")
	if err != nil {
		t.Fatal(err)
	}

	if w.lastReadOffset != 0 {
		t.Error("expected lastReadOffset to be 0, got %d", w.lastReadOffset)
	}

	entries := []walEntry{
		walEntry{
			operation: operationInsert,
			rows: Rows{
				Row{
					Source:    "hostA",
					Metric:    "metric.1",
					Timestamp: 123,
					Value:     0.234,
				},
			},
		},
		walEntry{
			operation: operationInsert,
			rows: Rows{
				Row{
					Source:    "hostA",
					Metric:    "metric.1",
					Timestamp: 456,
					Value:     0.234,
				},
			},
		},
		walEntry{
			operation: operationInsert,
			rows: Rows{
				Row{
					Source:    "hostA",
					Metric:    "metric.1",
					Timestamp: -456,
					Value:     -0.234,
				},
			},
		},
		walEntry{
			operation: operationInsert,
			rows: Rows{
				Row{
					Source:    "hostA",
					Metric:    "metric.1",
					Timestamp: 1000,
					Value:     -0.234,
				},
			},
		},
	}

	for _, entry := range entries {
		n, err := w.append(entry)
		if err != nil {
			t.Fatal(err)
		}

		if n == 0 {
			t.Errorf("expected to get non-zero bytes written, got %d", n)
		}
	}

	err = w.close()
	if err != nil {
		t.Fatal(err)
	}

	// Open it.
	log, err := openFileWAL("/tmp/TestMemoryPartitionRecover.wal")
	if err != nil {
		t.Fatal(err)
	}

	// We should recover state from the log.
	p, err := newMemoryPartition(log)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for a short amount of time
	// for the in-memory structure to update.
	time.Sleep(1 * time.Millisecond)

	expectedPoints := []point{
		point{
			Timestamp: 123,
			Value:     0.234,
		},
		point{
			Timestamp: 456,
			Value:     0.234,
		},
		point{
			Timestamp: 1000,
			Value:     -0.234,
		},
	}

	points, err := p.fetchPoints("hostA", "metric.1", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(points, expectedPoints) {
		t.Errorf("expected points %v, got %v", expectedPoints, points)
	}
}
