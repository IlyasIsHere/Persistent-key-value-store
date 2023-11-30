package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	ErrCorruptedFile   = errors.New("the file is corrupted")
	ErrKeyDeleted      = errors.New("the key was deleted")
	ErrOutdatedVersion = errors.New("the file version is not compatible with the current version")
)

type lsmDB struct {

	// The in-memory database
	memTable *MemTable

	// The WAL (Write-ahead log)
	wal *WAL

	// The magic number of our sst files
	magicNumber [4]byte

	// The version of the software
	version byte

	// The metadata file contains the current number of sst files
	// That number is represented as a 4 bytes integer inside the file
	metadataFileName string

	// The maximum number of bytes our memTable can hold before it is flushed to the disk
	memSizeThreshold int

	// The maximum number of sst files we can have before they are compacted into one sst file
	fileNumThreshold int

	// Path to sst files
	sstPath string

	// Number of current sst files
	sstFilesNum int
}

func (lsmdb *lsmDB) setCurrentSSTIndex() error {
	content, err := os.ReadFile(lsmdb.metadataFileName)
	if err != nil {
		return err
	}

	if len(content) != 4 {
		return ErrCorruptedFile
	}

	currIdx := binary.BigEndian.Uint32(content)
	lsmdb.sstFilesNum = int(currIdx)

	return nil
}

func (lsmdb *lsmDB) updateMetadataFile() error {
	file, err := os.OpenFile(lsmdb.metadataFileName, os.O_RDWR|os.O_TRUNC, 0666)
	defer file.Close()
	if err != nil {
		return err
	}

	if _, err := file.Write(encode4BytesInt(lsmdb.sstFilesNum)); err != nil {
		return err
	}

	return nil
}

// Returns a new header to be written to a new sst file.
// The sst files header is of this form: [magicNumber(4 bytes)][entryCount(4 bytes)]
// [lenSmallestKey(4 bytes)][SmallestKey][lenLargestKey(4 bytes)][largestKey][version(1 byte)]
func (lsmdb *lsmDB) createHeader() []byte {
	header := make([]byte, 0)
	header = append(header, lsmdb.magicNumber[:]...)
	header = append(header, encode4BytesInt(lsmdb.memTable.sortedMap.Len())...)

	smallestKey := []byte(lsmdb.memTable.sortedMap.Iterator().Key())
	lenSmallestKey := len(smallestKey)

	largestKey := []byte(lsmdb.memTable.sortedMap.Reverse().Key())
	lenLargestKey := len(largestKey)

	header = append(header, encode4BytesInt(lenSmallestKey)...)
	header = append(header, smallestKey...)
	header = append(header, encode4BytesInt(lenLargestKey)...)
	header = append(header, largestKey...)
	header = append(header, lsmdb.version)

	return header
}

// Flushes the current memTable to a new sst file, and clears the WAL.
func (lsmdb *lsmDB) flushToDisk() error {

	newSSTFilesNum := lsmdb.sstFilesNum + 1

	// Creating a new sst file
	sstName := fmt.Sprint(lsmdb.sstPath, "f", newSSTFilesNum, ".sst")
	sstFile, err := os.OpenFile(sstName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)

	if err != nil {
		return err
	}

	defer sstFile.Close()

	// Writing the header of the file
	header := lsmdb.createHeader()
	if _, err := sstFile.Write(header); err != nil {
		return err
	}

	// Flushing entries from memTable to the new sst file
	for it := lsmdb.memTable.sortedMap.Iterator(); it.Valid(); it.Next() {
		entry := lsmdb.memTable.makeEntry([]byte(it.Key()))
		encoded := entry.encode()

		if _, err := sstFile.Write(encoded); err != nil {
			return err
		}
	}

	lsmdb.sstFilesNum++
	// Updating the metadata file
	if err := lsmdb.updateMetadataFile(); err != nil {
		return err
	}

	// Clearing the wal
	if err := lsmdb.wal.clear(); err != nil {
		return err
	}

	return nil
}

// Loads the entries from the WAL to the MemTable
func (lsmdb *lsmDB) loadWALtoMemTable() error {
	// Seeking to the beginning of the WAL
	if _, err := lsmdb.wal.logFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Decoding every entry and loading it to the memTable
	for {
		op, key, value, err := decodeNext(lsmdb.wal.logFile)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		lsmdb.memTable.writeOperation(OperationType(op), key, value)
	}
}

// Search for a key in an sst file.
// Returns the value if the key is found, otherwise, if the key doesn't exist at all, returns nil, ErrKeyNotFound.
// Otherwise if the key was deleted, returns nil, ErrKeyDeleted.
func (lsmdb *lsmDB) searchSSTFile(sstFileNum int, key []byte) ([]byte, error) {
	filePath := fmt.Sprint(lsmdb.sstPath, "f", sstFileNum, ".sst")
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0600)

	defer file.Close()

	if err != nil {
		return nil, err
	}

	// Seeking to the beginning of the file
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// Reading the header of the sst file
	magicNumber, _, smallestKey, largestKey, version, err := readHeader(file)

	if err != nil {
		return nil, err
	}

	if !bytes.Equal(magicNumber, lsmdb.magicNumber[:]) {
		return nil, ErrCorruptedFile
	}

	if version != lsmdb.version {
		return nil, ErrOutdatedVersion
	}

	if bytes.Compare(key, smallestKey) < 0 || bytes.Compare(key, largestKey) > 0 {
		return nil, ErrKeyNotFound
	}

	// If we reach here, it means that the key may be in the sst file
	for {
		op, k, v, err := decodeNext(file)

		if err != nil {
			if err == io.EOF {
				return nil, ErrKeyNotFound
			}
			return nil, err
		}

		if bytes.Equal(k, key) {
			if OperationType(op) == DelOp {
				return nil, ErrKeyDeleted
			}
			return v, nil
		}
	}
}

func (lsmdb *lsmDB) searchAllSSTFiles(key []byte) ([]byte, error) {
	if err := lsmdb.setCurrentSSTIndex(); err != nil {
		return nil, err
	}

	// Start the search from the newest sst file
	for i := lsmdb.sstFilesNum; i >= 1; i-- {
		v, err := lsmdb.searchSSTFile(i, key)

		if err != nil {
			switch err {

			// The key was deleted, directly stop the search and return nil
			case ErrKeyDeleted:
				return nil, ErrKeyNotFound

			// The key is not found in the current sst file, move to the next one
			case ErrKeyNotFound:
				continue

			// Some other error happened
			default:
				return nil, err
			}
		}
		return v, nil
	}

	// We reach here if the key was not found in any sst file
	return nil, ErrKeyNotFound
}

func (lsmdb *lsmDB) Get(key []byte) ([]byte, error) {
	value, err := lsmdb.memTable.Get(key)
	switch err {
	// if the key exists in the memTable
	case nil:
		return value, nil

	// if the key is marked as deleted in the memTable
	case ErrKeyDeleted:
		return nil, ErrKeyNotFound

	// if the key is not found in the memTable
	default:
		return lsmdb.searchAllSSTFiles(key)
	}
}

func (lsmdb *lsmDB) Set(key, value []byte) error {
	entry := Entry{
		op:    SetOp,
		key:   key,
		value: value,
	}

	if err := lsmdb.wal.appendEntry(entry); err != nil {
		return err
	}

	lsmdb.memTable.Set(key, value)

	// If the memTable is full, flush it to the disk
	if lsmdb.memTable.sizeInBytes() >= lsmdb.memSizeThreshold {
		if err := lsmdb.flushToDisk(); err != nil {
			return err
		}

		// Clearing the memTable
		lsmdb.memTable.sortedMap.Clear()
	}

	return nil
}

func (lsmdb *lsmDB) Del(key []byte) ([]byte, error) {
	entry := Entry{
		op:    DelOp,
		key:   key,
		value: nil,
	}

	v, err := lsmdb.Get(key)
	if err != nil {
		return nil, err
	}

	if err := lsmdb.wal.appendEntry(entry); err != nil {
		return nil, err
	}

	lsmdb.memTable.Del(key)
	return v, nil
}

func (lsmdb *lsmDB) Open() error {

	// Creating the sst directory if it doesn't exist
	if _, err := os.Stat(lsmdb.sstPath); os.IsNotExist(err) {
		if err := os.Mkdir(lsmdb.sstPath, 0700); err != nil {
			return err
		}
	}

	// Creating the metadata file if it doesn't exist
	if _, err := os.Stat(lsmdb.metadataFileName); os.IsNotExist(err) {
		if _, err := os.Create(lsmdb.metadataFileName); err != nil {
			return err
		}

		// Writing the current number of sst files to the metadata file
		if err := lsmdb.updateMetadataFile(); err != nil {
			return err
		}
	}

	// Reading the current number of sst files from the metadata file
	if err := lsmdb.setCurrentSSTIndex(); err != nil {
		return err
	}

	if err := lsmdb.loadWALtoMemTable(); err != nil {
		return err
	}

	return nil
}
