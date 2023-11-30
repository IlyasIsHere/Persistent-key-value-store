package main

import (
	"os"
)

// Returns the magic number, the entry count, the smallest key, the largest key, and the version of the sst file.
func readHeader(file *os.File) ([]byte, int, []byte, []byte, byte, error) {

	part1 := make([]byte, 4+4+4)
	if _, err := file.Read(part1); err != nil {
		return nil, -1, nil, nil, 0, err
	}

	magicNumber := part1[:4]

	entryCount := decode4BytesInt(part1[4:8])
	lenSmallestKey := decode4BytesInt(part1[8:12])

	// Reading the smallest key
	smallestKey := make([]byte, lenSmallestKey)
	if _, err := file.Read(smallestKey); err != nil {
		return nil, -1, nil, nil, 0, err
	}

	part2 := make([]byte, 4)
	if _, err := file.Read(part2); err != nil {
		return nil, -1, nil, nil, 0, err
	}

	lenLargestKey := decode4BytesInt(part2)
	// Reading the largest key
	largestKey := make([]byte, lenLargestKey)
	if _, err := file.Read(largestKey); err != nil {
		return nil, -1, nil, nil, 0, err
	}

	// Reading the version
	version := make([]byte, 1)
	if _, err := file.Read(version); err != nil {
		return nil, -1, nil, nil, 0, err
	}

	return magicNumber, entryCount, smallestKey, largestKey, version[0], nil
}

// Decodes the next entry from the sst file.
// Returns the operation type, the key, the value, and the error.
func decodeNext(file *os.File) (byte, []byte, []byte, error) {
	opPart := make([]byte, 1)
	if _, err := file.Read(opPart); err != nil {
		return 0, nil, nil, err
	}
	op := opPart[0]

	keyLenPart := make([]byte, 4)
	if _, err := file.Read(keyLenPart); err != nil {
		return 0, nil, nil, err
	}
	keyLen := decode4BytesInt(keyLenPart)

	key := make([]byte, keyLen)
	if _, err := file.Read(key); err != nil {
		return 0, nil, nil, err
	}

	// If it's a del operation, we stop here
	if OperationType(op) == DelOp {
		return op, key, nil, nil
	}

	// Otherwise, if it's a set operation, we read the value
	valueLenPart := make([]byte, 4)
	if _, err := file.Read(valueLenPart); err != nil {
		return 0, nil, nil, err
	}
	valueLen := decode4BytesInt(valueLenPart)

	value := make([]byte, valueLen)
	if _, err := file.Read(value); err != nil {
		return 0, nil, nil, err
	}

	return op, key, value, nil
}
