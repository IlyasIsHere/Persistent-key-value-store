package main

import (
	"io"
	"os"
	"testing"
)

func TestReadHeader(t *testing.T) {
	// Create a temporary test file
	tempFile, err := os.CreateTemp("", "test_decode_read_header_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Write test data to the file
	testData := []byte{0x4C, 0x53, 0x4D, 0x44, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c', 0x00, 0x00, 0x00, 0x04, '1', '2', '3', '4', 0x01}
	_, err = tempFile.Write(testData)
	if err != nil {
		t.Fatal(err)
	}

	// Reset file offset for reading
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	// Call readHeader
	magicNumber, entryCount, smallestKey, largestKey, version, err := readHeader(tempFile)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the results
	expectedMagicNumber := []byte{0x4C, 0x53, 0x4D, 0x44}
	if string(magicNumber) != string(expectedMagicNumber) {
		t.Errorf("Expected magic number %v, got %v", expectedMagicNumber, magicNumber)
	}

	if entryCount != 2 {
		t.Errorf("Expected entry count 2, got %d", entryCount)
	}

	expectedSmallestKey := []byte("abc")
	if string(smallestKey) != string(expectedSmallestKey) {
		t.Errorf("Expected smallest key %s, got %s", expectedSmallestKey, smallestKey)
	}

	expectedLargestKey := []byte("1234")
	if string(largestKey) != string(expectedLargestKey) {
		t.Errorf("Expected largest key %s, got %s", expectedLargestKey, largestKey)
	}

	if version != 1 {
		t.Errorf("Expected version 1, got %d", version)
	}
}

func TestDecodeNext(t *testing.T) {
	// Create a temporary test file
	tempFile, err := os.CreateTemp("", "test_decode_next_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Write test data to the file
	testData := []byte{0x01, 0x00, 0x00, 0x00, 0x03, 'k', 'e', 'y', 0x00, 0x00, 0x00, 0x05, 'v', 'a', 'l', 'u', 'e'}
	_, err = tempFile.Write(testData)
	if err != nil {
		t.Fatal(err)
	}

	// Reset file offset for reading
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	// Call decodeNext
	op, key, value, err := decodeNext(tempFile)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the results
	if op != 1 {
		t.Errorf("Expected operation type 1, got %d", op)
	}

	expectedKey := []byte("key")
	if string(key) != string(expectedKey) {
		t.Errorf("Expected key %s, got %s", expectedKey, key)
	}

	expectedValue := []byte("value")
	if string(value) != string(expectedValue) {
		t.Errorf("Expected value %s, got %s", expectedValue, value)
	}
}

func TestDecodeNext2(t *testing.T) {
	// Create a temporary test file
	tempFile, err := os.CreateTemp("", "test_decode_next_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Write test data to the file
	testData := []byte{0x02, 0x00, 0x00, 0x00, 0x03, 'k', 'e', 'y'}
	_, err = tempFile.Write(testData)
	if err != nil {
		t.Fatal(err)
	}

	// Reset file offset for reading
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	// Call decodeNext
	op, key, value, err := decodeNext(tempFile)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the results
	if op != 2 {
		t.Errorf("Expected operation type 2, got %d", op)
	}

	expectedKey := []byte("key")
	if string(key) != string(expectedKey) {
		t.Errorf("Expected key %s, got %s", expectedKey, key)
	}

	if value != nil {
		t.Errorf("Expected value nil, got %s", value)
	}
}
