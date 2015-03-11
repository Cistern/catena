package partition

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/PreetamJinka/catena"
	"github.com/PreetamJinka/catena/wal"
)

func TestMemoryPartition1(t *testing.T) {
	timestamps := 100
	sources := 10
	metrics := 1000

	WAL, err := wal.NewFileWAL("/tmp/wal.wal")
	if err != nil {
		t.Fatal(err)
	}

	p := NewMemoryPartition(WAL)

	workQueue := make(chan []catena.Row, timestamps*sources)

	parallelism := 4
	runtime.GOMAXPROCS(parallelism)

	for i := 0; i < timestamps; i++ {
		for j := 0; j < sources; j++ {

			rows := make([]catena.Row, metrics)

			for k := 0; k < metrics; k++ {
				rows[k] = catena.Row{
					Source: "source_" + fmt.Sprint(j),
					Metric: "metric_" + fmt.Sprint(k),
					Point: catena.Point{
						Timestamp: int64(i),
						Value:     0,
					},
				}
			}

			workQueue <- rows
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(parallelism)

	start := time.Now()

	for i := 0; i < parallelism; i++ {
		go func() {
			for rows := range workQueue {
				err := p.InsertRows(rows)
				if err != nil {
					fmt.Println(err)
				}
			}
			wg.Done()
		}()
	}

	close(workQueue)

	wg.Wait()

	t.Logf("%0.2f rows / sec\n", float64(timestamps*sources*metrics)/time.Now().Sub(start).Seconds())

	i, err := p.NewIterator("source_0", "metric_0")
	if err != nil {
		t.Fatal(err)
	}

	expected := int64(0)
	for i.Next() == nil {
		if i.Point().Timestamp != expected {
			t.Fatalf("expected timestamp %d; got %d", expected, i.Point().Timestamp)
		}

		expected++
	}
	i.Close()
	if expected != int64(timestamps) {
		t.Fatal(expected)
	}

	WAL.Close()

	WAL, err = wal.OpenFileWAL("/tmp/wal.wal")
	if err != nil {
		t.Fatal(err)
	}

	start = time.Now()

	p, err = RecoverMemoryPartition(WAL)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%0.2f rows / sec\n", float64(timestamps*sources*metrics)/time.Now().Sub(start).Seconds())

	expected = int64(0)
	i, err = p.NewIterator("source_0", "metric_0")
	if err != nil {
		t.Fatal(err)
	}
	for i.Next() == nil {
		if i.Point().Timestamp != expected {
			t.Fatalf("expected timestamp %d; got %d", expected, i.Point().Timestamp)
		}

		expected++
	}
	i.Close()

	if expected != int64(timestamps) {
		t.Fatal(expected)
	}

	err = p.Destroy()
	if err != nil {
		t.Fatal(err)
	}
}
