package catena

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/PreetamJinka/catena/partition"
)

type partitionList struct {
	head unsafe.Pointer
	size int32
}

type partitionListNode struct {
	val  partition.Partition
	next unsafe.Pointer
}

type partitionListIterator struct {
	list    *partitionList
	current *partitionListNode
	valid   bool
}

func comparePartitions(a, b partition.Partition) int {
	// Highest timestamp first
	return int(b.MinTimestamp() - a.MinTimestamp())
}

func newPartitionList() *partitionList {
	return &partitionList{}
}

// Insert inserts v into the list in order. An error is returned if v is already present.
func (l *partitionList) Insert(v partition.Partition) error {
	n := &partitionListNode{
		val:  v,
		next: nil,
	}

HEAD:
	headPtr := atomic.LoadPointer(&l.head)

	if headPtr == nil {
		if !atomic.CompareAndSwapPointer(&l.head, headPtr, unsafe.Pointer(n)) {
			goto HEAD
		}

		atomic.AddInt32(&l.size, 1)
		return nil
	}

	headNode := (*partitionListNode)(headPtr)
	if comparePartitions(headNode.val, n.val) > 0 {
		n.next = headPtr
		if !atomic.CompareAndSwapPointer(&l.head, headPtr, unsafe.Pointer(n)) {
			goto HEAD
		}

		atomic.AddInt32(&l.size, 1)
		return nil
	}

NEXT:
	nextPtr := atomic.LoadPointer(&headNode.next)
	if nextPtr == nil {
		if !atomic.CompareAndSwapPointer(&headNode.next, nextPtr, unsafe.Pointer(n)) {
			goto NEXT
		}

		atomic.AddInt32(&l.size, 1)
		return nil
	}

	nextNode := (*partitionListNode)(nextPtr)
	if comparePartitions(nextNode.val, n.val) > 0 {
		n.next = nextPtr
		if !atomic.CompareAndSwapPointer(&headNode.next, nextPtr, unsafe.Pointer(n)) {
			goto NEXT
		}

		atomic.AddInt32(&l.size, 1)
		return nil
	}

	if comparePartitions(nextNode.val, n.val) == 0 {
		return errors.New("catena/partition_list: partition exists")
	}

	headNode = nextNode
	goto NEXT
}

func (l *partitionList) Swap(old, new partition.Partition) error {
	n := &partitionListNode{
		val:  new,
		next: nil,
	}

HEAD:
	headPtr := atomic.LoadPointer(&l.head)

	if headPtr == nil {
		return errors.New("catena/partition_list: partition not found")
	}

	headNode := (*partitionListNode)(headPtr)
	if comparePartitions(headNode.val, n.val) == 0 {
		n.next = headNode.next

		if !atomic.CompareAndSwapPointer(&l.head, headPtr, unsafe.Pointer(n)) {
			goto HEAD
		}

		return nil
	}

NEXT:
	nextPtr := atomic.LoadPointer(&headNode.next)
	if nextPtr == nil {
		return errors.New("catena/partition_list: partition not found")
	}

	nextNode := (*partitionListNode)(nextPtr)
	if comparePartitions(nextNode.val, n.val) == 0 {
		n.next = nextNode.next

		if !atomic.CompareAndSwapPointer(&headNode.next, nextPtr, unsafe.Pointer(n)) {
			goto NEXT
		}

		return nil
	}

	if comparePartitions(nextNode.val, n.val) > 0 {
		return errors.New("catena/partition_list: partition not found")
	}

	headNode = nextNode
	goto NEXT
}

// Remove removes v from the list. An error is returned if v is not present.
func (l *partitionList) Remove(v partition.Partition) error {
HEAD:
	headPtr := atomic.LoadPointer(&l.head)

	if headPtr == nil {
		return errors.New("catena/partition_list: partition not found")
	}

	headNode := (*partitionListNode)(headPtr)

	if comparePartitions(headNode.val, v) == 0 {
		nextPtr := atomic.LoadPointer(&headNode.next)
		if !atomic.CompareAndSwapPointer(&l.head, headPtr, nextPtr) {
			goto HEAD
		}

		atomic.AddInt32(&l.size, -1)
		return nil
	}

NEXT:
	nextPtr := atomic.LoadPointer(&headNode.next)
	if nextPtr == nil {
		return errors.New("catena/partition_list: partition not found")
	}

	nextNode := (*partitionListNode)(nextPtr)

	if comparePartitions(nextNode.val, v) > 0 {
		return errors.New("catena/partition_list: partition not found")
	}

	if comparePartitions(nextNode.val, v) == 0 {
		replacementPtr := atomic.LoadPointer(&nextNode.next)
		if !atomic.CompareAndSwapPointer(&headNode.next, nextPtr, replacementPtr) {
			goto NEXT
		}

		atomic.AddInt32(&l.size, -1)
		return nil
	}

	headNode = nextNode
	goto NEXT
}

// Size returns the number of elements currently in the list.
func (l *partitionList) Size() int {
	return int(atomic.LoadInt32(&l.size))
}

// NewIterator returns a new iterator. Values can be read
// after Next is called.
func (l *partitionList) NewIterator() *partitionListIterator {
	return &partitionListIterator{
		list:  l,
		valid: true,
	}
}

// Next positions the iterator at the next node in the list.
// Next will be positioned at the head on the first call.
// The return value will be true if a value can be read from the list.
func (i *partitionListIterator) Next() bool {
	if !i.valid {
		return false
	}

	if i.current == nil {
		head := atomic.LoadPointer(&i.list.head)
		if head == nil {
			i.valid = false
			return false
		}

		i.current = (*partitionListNode)(head)
		return true
	}

	next := atomic.LoadPointer(&i.current.next)
	i.current = (*partitionListNode)(next)

	i.valid = i.current != nil
	return i.valid
}

func (i *partitionListIterator) HasNext() bool {
	if i.current == nil {
		return false
	}

	return atomic.LoadPointer(&i.current.next) != nil
}

// Value reads the value from the current node of the iterator.
// An error is returned if a value cannot be retrieved.
func (i *partitionListIterator) Value() (partition.Partition, error) {
	var v partition.Partition

	if i.current == nil {
		return v, errors.New("catena/partition_list: partition not found")
	}

	return i.current.val, nil
}

// String returns the string representation of the list.
func (l *partitionList) String() string {
	output := ""

	if l.head == nil {
		return output
	}

	i := l.NewIterator()

	for i.Next() {
		v, _ := i.Value()
		output += fmt.Sprintf("%v ", v)
	}

	return output
}
