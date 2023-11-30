package main

import "os"

func main() {
	memTable := newMemTable()

	logfile, ferr := os.OpenFile("wal.log", os.O_RDWR|os.O_CREATE, 0600)
	defer logfile.Close()

	if ferr != nil {
		panic(ferr)
	}

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

	if err := lsmdb.Open(); err != nil {
		panic(err)
	}

	// Launching the HTTP API
	handleRequests(&lsmdb)

}
