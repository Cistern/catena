package catena

import (
	"os"
	"testing"
)

func TestPartition(t *testing.T) {
	f, err := os.Create("/tmp/catena.test.partition")
	if err != nil {
		t.Fatal(err)
	}

	mp := newMemoryPartition()
	mp.addPoint("sourceA", "metricA", 0, 1)
	mp.addPoint("sourceA", "metricA", -1, 1)
	mp.addPoint("sourceA", "metricA", 1, 1)
	mp.addPoint("sourceA", "metricA", -2, 1)
	mp.addPoint("sourceA", "metricA", 2, 1)
	mp.addPoint("sourceA", "metricA", 3, 1)
	mp.addPoint("sourceA", "metricA", 4, 1)
	mp.addPoint("sourceA", "metricA", -5, 1)

	t.Log(mp)

	// TODO: check values

	mp.ro = true
	t.Log(mp.serialize(f))

	f.Close()

	fp, err := openFilePartition("/tmp/catena.test.partition")
	if err != nil {
		t.Fatal(err)
	}

	t.Log(fp.loadMetadata())

	t.Log(fp.fetchPoints("sourceA", "metricA", -100, 100))
}
