package catena

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/PreetamJinka/catena/partition"
	"github.com/PreetamJinka/catena/partition/disk"
	"github.com/PreetamJinka/catena/partition/memory"
	"github.com/PreetamJinka/catena/wal"
)

func TestMemoryPartition1(t *testing.T) {
	timestamps := 100
	sources := 100
	metrics := 100

	WAL, err := wal.NewFileWAL("/tmp/wal.wal")
	if err != nil {
		t.Fatal(err)
	}

	p := memory.NewMemoryPartition(WAL)

	workQueue := make(chan []partition.Row, timestamps*sources)

	parallelism := runtime.NumCPU()
	runtime.GOMAXPROCS(parallelism)

	for i := 0; i < timestamps; i++ {
		for j := 0; j < sources; j++ {

			rows := make([]partition.Row, metrics)

			for k := 0; k < metrics; k++ {
				rows[k] = partition.Row{
					Source: "source_" + fmt.Sprint(j),
					Metric: "metric_" + fmt.Sprint(k),
					Point: partition.Point{
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
					t.Fatal(err)
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

	p.Close()

	WAL, err = wal.OpenFileWAL("/tmp/wal.wal")
	if err != nil {
		t.Fatal(err)
	}

	start = time.Now()

	p, err = memory.RecoverMemoryPartition(WAL)
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

	p.SetReadOnly()

	f, err := os.Create("/tmp/compact.part")
	if err != nil {
		p.Destroy()
		t.Fatal(err)
	}

	err = p.Compact(f)
	if err != nil {
		p.Destroy()
		t.Fatal(err)
	}

	err = p.Destroy()
	if err != nil {
		t.Fatal(err)
	}

	f.Close()

	d, err := disk.OpenDiskPartition("/tmp/compact.part")
	if err != nil {
		t.Fatal(err)
	}

	diskIter, err := d.NewIterator("source_0", "metric_0")
	if err != nil {
		t.Fatal(err)
	}

	expected = 0
	for diskIter.Next() == nil {
		if diskIter.Point().Timestamp != expected {
			t.Fatalf("expected timestamp %d; got %d", expected, diskIter.Point().Timestamp)
		}

		expected++
	}
	diskIter.Close()

	err = d.Destroy()
	if err != nil {
		t.Fatal(err)
	}
}
