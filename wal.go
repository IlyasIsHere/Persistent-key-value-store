package main

import (
	"encoding/binary"
	"io"
	"os"
)

// Set or del entry. For del entries, field "value" is nil.
type Entry struct {
	op    OperationType
	key   []byte
	value []byte
}

// Write-ahead log
type WAL struct {
	logFile *os.File
	walPath string
}

// Clears the WAL file (closes the current open wal file, and opens a new one in truncate mode)
func (wal *WAL) clear() error {
	if err := wal.logFile.Close(); err != nil {
		return err
	}

	wal.logFile = nil

	newWal, err := os.OpenFile(wal.walPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	wal.logFile = newWal
	return nil
}

// Writes entry to the end of the WAL
func (wal *WAL) appendEntry(entry Entry) error {
	if _, err := wal.logFile.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	encodedEntry := entry.encode()
	if _, err := wal.logFile.Write(encodedEntry); err != nil {
		return err
	}

	return nil
}

// The format is the following: (1 byte for operation type, 4 bytes for key length, 4 bytes for value length).
//
// For a delete record: [DelOp][Key length][Key]
//
// For a set record: 		[SetOp][Key length][Key][Value length][Value]
func (entry *Entry) encode() []byte {

	keyLen := len(entry.key)

	encoded := []byte{byte(entry.op)}
	encodedKeyLen := encode4BytesInt(keyLen)
	encoded = append(encoded, encodedKeyLen...)
	encoded = append(encoded, entry.key...)

	if entry.op == SetOp {
		valueLen := len(entry.value)
		encodedValueLen := encode4BytesInt(valueLen)
		encoded = append(encoded, encodedValueLen...)
		encoded = append(encoded, entry.value...)
	}

	return encoded
}

func encode4BytesInt(n int) []byte {
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, uint32(n))

	return encoded
}

func decode4BytesInt(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
}
