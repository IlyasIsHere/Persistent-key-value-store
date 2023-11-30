package main

import (
	"bytes"
	"os"
	"testing"
)

func TestWAL_clear(t *testing.T) {
	// Create a temporary test WAL file
	tempFile, err := os.CreateTemp("", "test_wal_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	wal := WAL{
		logFile: tempFile,
		walPath: tempFile.Name(),
	}
	defer os.Remove(wal.walPath)
	defer wal.logFile.Close()

	// Write some data to the file
	data := []byte("test data")
	_, err = tempFile.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	// Call clear
	err = wal.clear()
	if err != nil {
		t.Fatal(err)
	}

	// Check if the file is empty after clear
	fileInfo, err := wal.logFile.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if fileInfo.Size() != 0 {
		t.Errorf("Expected file size 0 after clear, got %d", fileInfo.Size())
	}
}

func TestWAL_appendEntry(t *testing.T) {
	// Create a temporary test WAL file
	tempFile, err := os.CreateTemp("", "test_wal_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	wal := WAL{
		logFile: tempFile,
		walPath: tempFile.Name(),
	}

	entry := Entry{
		op:    SetOp,
		key:   []byte("testKey"),
		value: []byte("testValue"),
	}

	// Call appendEntry
	err = wal.appendEntry(entry)
	if err != nil {
		t.Fatal(err)
	}

	// Read the content of the file and check if it matches the expected encoded entry
	fileContent, err := os.ReadFile(tempFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	expectedEncodedEntry := entry.encode()
	if !bytes.Equal(fileContent, expectedEncodedEntry) {
		t.Errorf("Expected file content %v, got %v", expectedEncodedEntry, fileContent)
	}
}
