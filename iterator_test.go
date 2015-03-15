package catena

import (
	"os"
	"testing"
	"time"

	"github.com/PreetamJinka/catena/partition"
)

func TestIterator(t *testing.T) {
	os.RemoveAll("/tmp/catena_iterator_test")

	db, err := NewDB("/tmp/catena_iterator_test", 5, 4)
	if err != nil {
		t.Fatal(err)
	}

	insert5Rows := func(startingTS int64) error {
		return db.InsertRows([]Row{
			Row{
				Source: "a",
				Metric: "b",
				Point: partition.Point{
					Timestamp: startingTS,
				},
			},
			Row{
				Source: "a",
				Metric: "b",
				Point: partition.Point{
					Timestamp: startingTS + 1,
				},
			},
			Row{
				Source: "a",
				Metric: "b",
				Point: partition.Point{
					Timestamp: startingTS + 2,
				},
			},
			Row{
				Source: "a",
				Metric: "b",
				Point: partition.Point{
					Timestamp: startingTS + 3,
				},
			},
			Row{
				Source: "a",
				Metric: "b",
				Point: partition.Point{
					Timestamp: startingTS + 4,
				},
			},
		})
	}

	err = insert5Rows(0)
	if err != nil {
		t.Fatal(err)
	}
	err = insert5Rows(5)
	if err != nil {
		t.Fatal(err)
	}
	err = insert5Rows(10)
	if err != nil {
		t.Fatal(err)
	}
	err = insert5Rows(15)
	if err != nil {
		t.Fatal(err)
	}

	db.compact()

	i, err := db.NewIterator("b", "b")
	if err == nil {
		t.Fatal("expected to see an error for an invalid iterator")
	}

	i, err = db.NewIterator("a", "b")
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 0
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	if i.Point().Timestamp != 0 {
		t.Fatalf("expected timestamp %d, got %d", 0, i.Point().Timestamp)
	}

	// timestamp 1
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 2
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 3
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 4
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	if i.Point().Timestamp != 4 {
		t.Fatalf("expected timestamp %d, got %d", 4, i.Point().Timestamp)
	}

	// Seek to point 12
	err = i.Seek(12)
	if err != nil {
		t.Fatal(err)
	}

	if i.Point().Timestamp != 12 {
		t.Fatalf("expected timestamp %d, got %d", 12, i.Point().Timestamp)
	}

	err = i.Seek(2)
	if err != nil {
		t.Fatal(err)
	}

	if i.Point().Timestamp != 2 {
		t.Fatalf("expected timestamp %d, got %d", 2, i.Point().Timestamp)
	}

	// Now we add some more points.
	err = insert5Rows(20)
	if err != nil {
		t.Fatal(err)
	}

	// Wait a bit for the compactor to start up.
	// It should be blocked on the iterator.
	time.Sleep(time.Millisecond * 100)

	// Close the iterator, which should unblock the compactor.
	i.Close()

	// Let the compactor do its work.
	time.Sleep(time.Millisecond * 100)

	i, err = db.NewIterator("a", "b")
	if err != nil {
		t.Fatal(err)
	}

	// Now we should be at point 5 because the oldest partition got dropped.
	if i.Point().Timestamp != 5 {
		t.Fatalf("expected timestamp %d, got %d", 5, i.Point().Timestamp)
	}

	err = i.Seek(0)
	// Note that this should NOT be an error. Seek moves the iterator to the
	// first timestamp greater than or equal to the requested timestamp,
	// so we should now be at timestamp 5.
	if err != nil {
		t.Fatal(err)
	}

	if i.Point().Timestamp != 5 {
		t.Fatalf("expected timestamp %d, got %d", 5, i.Point().Timestamp)
	}

	err = i.Seek(5)
	if err != nil {
		t.Fatal(err)
	}

	if i.Point().Timestamp != 5 {
		t.Fatalf("expected timestamp %d, got %d", 5, i.Point().Timestamp)
	}

	// timestamp 6
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 7
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 8
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 9
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// Crossing partition boundary

	// timestamp 10
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 11
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	if i.Point().Timestamp != 11 {
		t.Fatalf("expected timestamp %d, got %d", 11, i.Point().Timestamp)
	}

	// timestamp 12
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 13
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 14
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// Crossing partition boundary

	// timestamp 15
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	// timestamp 16
	err = i.Next()
	if err != nil {
		t.Fatal(err)
	}

	if i.Point().Timestamp != 16 {
		t.Fatalf("expected timestamp %d, got %d", 16, i.Point().Timestamp)
	}

	i.Close()
}
