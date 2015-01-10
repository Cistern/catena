package catena

import (
	"os"
	"testing"
)

func TestCatena(t *testing.T) {
	const numPoints = 10000

	os.RemoveAll("/tmp/catena/1")
	db, err := NewDB("/tmp/catena/1")
	if err != nil {
		t.Fatal(err)
	}

	db.partitionModulus = numPoints / 8

	t.Log(db.InsertRows(Rows{
		Row{
			Source:    "hostA",
			Metric:    "metric.1",
			Timestamp: 1,
			Value:     0.234,
		},
		Row{
			Source:    "hostA",
			Metric:    "metric.3",
			Timestamp: 1,
			Value:     0.234,
		},
	}))

	for i := int64(2); i <= numPoints; i++ {
		err := db.InsertRows(Rows{
			Row{
				Source:    "hostA",
				Metric:    "metric.1",
				Timestamp: i,
				Value:     0.234,
			},
			Row{
				Source:    "hostB",
				Metric:    "metric.1",
				Timestamp: i,
				Value:     0.234,
			},
			Row{
				Source:    "hostA",
				Metric:    "metric.2",
				Timestamp: i,
				Value:     0.234,
			},
			Row{
				Source:    "hostA",
				Metric:    "metric.0",
				Timestamp: i,
				Value:     0.234,
			},
			Row{
				Source:    "hostA",
				Metric:    "metric.3",
				Timestamp: i,
				Value:     0.234,
			},
		})

		if err != nil {
			t.Fatal(err)
		}
	}

	resp := db.Query([]QueryDesc{
		QueryDesc{
			Source: "hostA",
			Metric: "metric.1",
			Start:  -10000,
			End:    100000,
		},
		QueryDesc{
			Source: "hostA",
			Metric: "metric.3",
			Start:  -10000,
			End:    100000,
		},
	})

	if len(resp.Series) != 2 {
		t.Fatalf("Expected 2 series, got %d", len(resp.Series))
	}

	s1 := resp.Series[0]
	s2 := resp.Series[1]

	if len(s1.Points) != numPoints {
		t.Errorf("Expected %d points for s1, got %d", numPoints, len(s1.Points))
	}

	if len(s2.Points) != numPoints {
		t.Errorf("Expected %d points for s2, got %d", numPoints, len(s2.Points))
	}
}
