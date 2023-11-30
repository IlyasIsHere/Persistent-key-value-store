package main

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

func TestFlushToDisk(t *testing.T) {

	// Create a temporary test directory
	tempDir, err := os.MkdirTemp("", "test_flush_to_disk_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a sample lsmDB instance
	memTable := newMemTable()
	logfile, ferr := os.CreateTemp("", "test_log_*.log")
	if ferr != nil {
		t.Fatal(ferr)
	}
	defer os.Remove(logfile.Name())
	defer logfile.Close()

	wal := WAL{
		logFile: logfile,
		walPath: logfile.Name(),
	}

	metaFile, ferr := os.CreateTemp("", "test_meta_*.meta")
	if ferr != nil {
		t.Fatal(ferr)
	}
	defer os.Remove(metaFile.Name())
	defer metaFile.Close()

	lsmdb := lsmDB{
		memTable:         &memTable,
		wal:              &wal,
		magicNumber:      [4]byte{0x4c, 0x53, 0x4d, 0x44},
		version:          1,
		metadataFileName: metaFile.Name(),
		memSizeThreshold: 100,
		fileNumThreshold: 20,
		sstPath:          tempDir + "/",
		sstFilesNum:      0,
	}

	// Set up memTable entries
	lsmdb.Set([]byte("key1"), []byte("value1"))
	lsmdb.Set([]byte("key2"), []byte("value2"))

	// Call flushToDisk
	err = lsmdb.flushToDisk()
	if err != nil {
		t.Fatal(err)
	}

	// Verify the created SST file
	sstFilePath := tempDir + "/f1.sst"
	sstFile, err := os.Open(sstFilePath)
	if err != nil {
		t.Fatal(err)
	}
	defer sstFile.Close()

	// Verify the header
	expectedHeader := []byte{0x4c, 0x53, 0x4d, 0x44, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 'k', 'e', 'y', '1', 0x00, 0x00, 0x00, 0x04, 'k', 'e', 'y', '2', 0x01}
	header := make([]byte, len(expectedHeader))
	_, err = sstFile.Read(header)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(header, expectedHeader) {
		t.Errorf("Expected header %v, got %v", expectedHeader, header)
	}

	// Verify the entries
	var entries []Entry
	for {
		op, key, value, err := decodeNext(sstFile)
		if err != nil {
			break
		}
		entries = append(entries, Entry{OperationType(op), key, value})
	}

	// Verify the entries
	expectedEntries := []Entry{
		{SetOp, []byte("key1"), []byte("value1")},
		{SetOp, []byte("key2"), []byte("value2")},
	}
	if !reflect.DeepEqual(entries, expectedEntries) {
		t.Errorf("Expected entries %v, got %v", expectedEntries, entries)
	}
}

func TestLoadWALtoMemTable(t *testing.T) {
	// Create a sample lsmDB instance
	memTable := newMemTable()
	logfile, ferr := os.CreateTemp("", "test_log_*.log")
	if ferr != nil {
		t.Fatal(ferr)
	}
	defer os.Remove(logfile.Name())
	defer logfile.Close()

	wal := WAL{
		logFile: logfile,
		walPath: "wal.log",
	}

	lsmdb := lsmDB{
		memTable:         &memTable,
		wal:              &wal,
		magicNumber:      [4]byte{0x4c, 0x53, 0x4d, 0x44},
		version:          1,
		metadataFileName: "metadata.meta",
		memSizeThreshold: 100,
		fileNumThreshold: 20,
		sstPath:          "sst/",
		sstFilesNum:      0,
	}

	// Add some entries to the WAL
	entry1 := Entry{SetOp, []byte("key1"), []byte("value1")}
	entry2 := Entry{SetOp, []byte("key2"), []byte("value2")}
	entry3 := Entry{DelOp, []byte("key1"), nil}

	err := lsmdb.wal.appendEntry(entry1)
	if err != nil {
		t.Fatal(err)
	}
	err = lsmdb.wal.appendEntry(entry2)
	if err != nil {
		t.Fatal(err)
	}
	err = lsmdb.wal.appendEntry(entry3)
	if err != nil {
		t.Fatal(err)
	}

	// Call loadWALtoMemTable
	err = lsmdb.loadWALtoMemTable()
	if err != nil {
		t.Fatal(err)
	}

	// Verify the entries in the memTable
	expectedEntries := []Entry{
		{DelOp, []byte("key1"), nil},
		{SetOp, []byte("key2"), []byte("value2")},
	}

	got := make([]Entry, 0)
	for it := memTable.sortedMap.Iterator(); it.Valid(); it.Next() {
		gottenKey := []byte(it.Key())
		gottenOp, gottenValue := parseInMemValue(it.Value())
		got = append(got, Entry{gottenOp, gottenKey, gottenValue})
	}

	if !reflect.DeepEqual(got, expectedEntries) {
		t.Errorf("Expected entries %v, got %v", expectedEntries, got)
	}
}

func TestLSMTreeDB(t *testing.T) {
	memTable := newMemTable()

	logfile, ferr := os.CreateTemp("", "test_log_*.log")
	if ferr != nil {
		t.Fatal(ferr)
	}
	defer os.Remove(logfile.Name())
	defer logfile.Close()

	tempDir, err := os.MkdirTemp("", "test_main_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	metaFile, ferr := os.CreateTemp("", "test_meta_*.meta")
	if ferr != nil {
		t.Fatal(ferr)
	}

	metaFile.Write([]byte{0x00, 0x00, 0x00, 0x00})
	defer os.Remove(metaFile.Name())
	defer metaFile.Close()

	wal := WAL{
		logFile: logfile,
		walPath: logfile.Name(),
	}

	lsmdb := lsmDB{
		memTable:         &memTable,
		wal:              &wal,
		magicNumber:      [4]byte{0x4c, 0x53, 0x4d, 0x44},
		version:          1,
		metadataFileName: metaFile.Name(),
		memSizeThreshold: 100,
		fileNumThreshold: 20,
		sstPath:          tempDir + "/",
		sstFilesNum:      0,
	}

	if err := lsmdb.Open(); err != nil {
		t.Fatal(err)
	}

	if err := lsmdb.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := lsmdb.Set([]byte("key2"), []byte("value2")); err != nil {
		t.Fatal(err)
	}

	if v, err := lsmdb.Get([]byte("key1")); err != nil || string(v) != "value1" {
		t.Error("Expected value1, got error or different value")
	}

	if v, err := lsmdb.Del([]byte("key2")); err != nil || string(v) != "value2" {
		t.Error("Expected value2, got error or different value")
	}

	if _, err := lsmdb.Get([]byte("key2")); err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

}
