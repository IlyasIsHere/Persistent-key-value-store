package main

import (
	"reflect"
	"testing"
)

func TestMemTable_SetAndGet(t *testing.T) {
	mem := newMemTable()

	key := []byte("testKey")
	value := []byte("testValue")

	// Test Set
	mem.Set(key, value)

	// Test Get
	result, err := mem.Get(key)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if !reflect.DeepEqual(result, value) {
		t.Errorf("Expected value %v, got %v", value, result)
	}
}

func TestMemTable_GetNotFound(t *testing.T) {
	mem := newMemTable()

	key := []byte("nonexistentKey")

	// Test Get when key is not found
	_, err := mem.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Expected error %v, got %v", ErrKeyNotFound, err)
	}
}

func TestMemTable_GetDeleted(t *testing.T) {
	mem := newMemTable()

	key := []byte("deletedKey")
	value := []byte("deletedValue")

	// Set and then delete
	mem.Set(key, value)
	mem.Del(key)

	// Test Get on a deleted key
	_, err := mem.Get(key)
	if err != ErrKeyDeleted {
		t.Errorf("Expected error %v, got %v", ErrKeyDeleted, err)
	}
}

func TestMemTable_Del(t *testing.T) {
	mem := newMemTable()

	key := []byte("keyToDelete")
	value := []byte("valueToDelete")

	// Set and then delete
	mem.Set(key, value)
	mem.Del(key)

	// Test Get after deletion
	_, err := mem.Get(key)
	if err != ErrKeyDeleted {
		t.Errorf("Expected error %v, got %v", ErrKeyDeleted, err)
	}
}

func TestMemTable_SizeInBytes(t *testing.T) {
	mem := newMemTable()

	// Test SizeInBytes for an empty MemTable
	size := mem.sizeInBytes()
	if size != 0 {
		t.Errorf("Expected size 0, got %d", size)
	}

	// Add an entry and test again
	key := []byte("testKey")
	value := []byte("testValue")
	mem.Set(key, value)

	size = mem.sizeInBytes()
	expectedSize := len(key) + len(value) + 1 // 1 byte for the operation type
	if size != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, size)
	}
}
