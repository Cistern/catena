package catena

import (
	"testing"
)

func TestPartition(t *testing.T) {
	mp := NewMemoryPartition()
	mp.addPoint("A", "1", 1, 1)
	mp.addPoint("C", "1", 1, 1)
	mp.addPoint("A", "2", 1, 1)
	mp.addPoint("A", "1", 100000, 1)
	t.Log(mp)

	// TODO: check values
}
