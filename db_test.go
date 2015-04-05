package catena

import (
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

func TestDB(t *testing.T) {
	os.RemoveAll("/tmp/catena")

	db, err := NewDB("/tmp/catena", 500, 20)
	if err != nil {
		t.Fatal(err)
	}

	ts := int64(0)

	parallelism := 4
	runtime.GOMAXPROCS(parallelism)
	wg := sync.WaitGroup{}
	wg.Add(parallelism)

	work := make(chan []Row, parallelism)

	for i := 0; i < parallelism; i++ {
		go func() {
			for rows := range work {
				err := db.InsertRows(rows)
				if err != nil {
					wg.Done()
					t.Fatal(err)
				}
			}

			wg.Done()
		}()
	}

	for n := 0; n < 500; n++ {

		rows := []Row{}
		for i := 0; i < 10000; i++ {
			rows = append(rows, Row{
				Source: "src",
				Metric: "met_" + strconv.Itoa(i),
				Point: Point{
					Timestamp: ts,
					Value:     float64(i),
				},
			})
		}

		ts++

		work <- rows
	}

	close(work)
	wg.Wait()

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}
