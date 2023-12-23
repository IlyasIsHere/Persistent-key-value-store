# LSM DB (Log-Structured Merge Database)

LSM DB is a minimalistic Log-Structured Merge (LSM) Database written in Go. It provides a simple key-value store with persistence and a basic HTTP API for interaction.

## Features

- In-memory MemTable for fast read and write operations.
- Write-Ahead Log (WAL) for durability.
- Persistent in-disk storage in SST files (Sorted String Files).
- Basic HTTP API for Set, Get, and Delete operations.

## HTTP API endpoints
#### Retrieve the value associated with the specified key
- GET ```http://localhost:8080/get?key=keyName```
#### Set a key-value pair encoded in JSON in the request body
- POST ```http://localhost:8080/set```
#### Delete the key-value pair with the specified key
- DELETE ```http://localhost:8080/del?key=keyName```

## Notes
The in-memory MemTable uses a sorted treemap from: [github.com/igrmk/treemap/](https://github.com/igrmk/treemap/)

## Improvements (not yet implemented)
- Compaction: merging sst files into one sst file by removing duplicates, etc.
- Bloom Filter: We can use bloom filters in sst files for faster check of key existence.
- Compression: sst files could be compressed to save more storage.

#### Feel free to contribute to this project by opening issues, providing suggestions, or submitting pull requests. Your contributions are highly valued!
