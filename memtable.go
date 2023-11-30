package main

import (
	"errors"
	"github.com/igrmk/treemap/v2"
)

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrKeyNotFoundInMap = errors.New("key doesn't exist in the sorted map")
)

type OperationType byte

const (
	SetOp OperationType = 1
	DelOp OperationType = 2
)

// Entries in the MemTable are of this format: {key: [DelOp]} or {key: [SetOp] + [value]}
// This format is chosen to distinguish between set and deleted keys.
type MemTable struct {
	sortedMap treemap.TreeMap[string, []byte]
}

func (mem *MemTable) Set(key, value []byte) error {
	valueWithOp := append([]byte{byte(SetOp)}, value...)
	mem.sortedMap.Set(string(key), valueWithOp)

	return nil
}

func (mem *MemTable) Get(key []byte) ([]byte, error) {

	if valueWithOp, ok := mem.sortedMap.Get(string(key)); ok {

		op, actualValue := parseInMemValue(valueWithOp)

		if op == SetOp {
			return actualValue, nil
		} else if op == DelOp {
			return nil, ErrKeyDeleted
		}
	}

	return nil, ErrKeyNotFound
}

func parseInMemValue(inMemValue []byte) (OperationType, []byte) {
	if inMemValue[0] == byte(DelOp) {
		return OperationType(inMemValue[0]), nil
	}
	return OperationType(inMemValue[0]), inMemValue[1:]
}

func (mem *MemTable) writeOperation(op OperationType, key []byte, value []byte) {
	switch op {
	case SetOp:
		valueWithOp := append([]byte{byte(SetOp)}, value...)
		mem.sortedMap.Set(string(key), valueWithOp)

	case DelOp:
		mem.sortedMap.Set(string(key), []byte{byte(DelOp)})
	}
}

func (mem *MemTable) Del(key []byte) {

	mem.sortedMap.Set(string(key), []byte{byte(DelOp)})

}

func (mem *MemTable) makeEntry(key []byte) Entry {
	valueWithOp, _ := mem.sortedMap.Get(string(key))
	op, actualValue := parseInMemValue(valueWithOp)

	entry := Entry{
		op:    op,
		key:   key,
		value: actualValue,
	}

	return entry
}

func (mem *MemTable) sizeInBytes() int {
	size := 0
	for it := mem.sortedMap.Iterator(); it.Valid(); it.Next() {
		size += len(it.Key()) + len(it.Value())
	}
	return size
}

func newMemTable() MemTable {
	sortedmap := treemap.New[string, []byte]()
	memTable := MemTable{
		sortedMap: *sortedmap,
	}

	return memTable
}
