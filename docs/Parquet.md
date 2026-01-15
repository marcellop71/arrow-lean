# Parquet Support

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format optimized for analytical workloads. Arrow-Lean provides bindings for reading and writing Parquet files through the Arrow C++ libraries.

## Overview

Parquet complements Arrow by providing:

- **Persistent storage** for Arrow's in-memory columnar format
- **Compression** (Snappy, GZIP, ZSTD, LZ4, Brotli)
- **Predicate pushdown** for efficient filtering
- **Column pruning** to read only needed data
- **Row group organization** for parallel processing

## Installation

Parquet support requires the Apache Arrow C++ libraries:

**Ubuntu/Debian:**
```bash
sudo apt-get install libarrow-dev libparquet-dev
```

**macOS:**
```bash
brew install apache-arrow
```

## Reading Parquet Files

### Basic Reading

```lean
import ArrowLean

def readParquet (path : String) : IO Unit := do
  let reader_opt ← ParquetReader.open path
  match reader_opt with
  | none => Zlog.error s!"Failed to open: {path}"
  | some reader => do
    let stream_opt ← reader.readTable
    match stream_opt with
    | some stream =>
      stream.forEachArray fun array => do
        Zlog.info s!"Batch: {array.length} rows"
    | none =>
      Zlog.error "Failed to read table"
    reader.close
```

### Reading Specific Columns

Read only the columns you need to minimize I/O and memory:

```lean
def readColumns (path : String) : IO Unit := do
  let reader_opt ← ParquetReader.open path
  match reader_opt with
  | some reader => do
    let columns := #["timestamp", "price", "volume"]
    let stream_opt ← reader.readColumns columns
    match stream_opt with
    | some stream =>
      Zlog.info "Reading selected columns"
      -- Process filtered data...
    | none =>
      Zlog.error "Failed to read columns"
    reader.close
  | none =>
    Zlog.error "Failed to open file"
```

### Reading Row Groups

Process large files in chunks using row groups:

```lean
def readByRowGroup (path : String) : IO Unit := do
  let reader_opt ← ParquetReader.open path
  match reader_opt with
  | some reader => do
    let metadata_opt ← reader.getMetadata
    match metadata_opt with
    | some metadata => do
      Zlog.info s!"File has {metadata.num_row_groups} row groups"
      for i in [:metadata.num_row_groups.toNat] do
        let stream_opt ← reader.readRowGroup i.toUInt32
        match stream_opt with
        | some stream =>
          Zlog.debug s!"Processing row group {i}"
          -- Process row group...
        | none =>
          Zlog.warn s!"Failed to read row group {i}"
    | none =>
      Zlog.error "No metadata"
    reader.close
  | none =>
    Zlog.error "Failed to open file"
```

### Accessing Metadata

```lean
def printMetadata (path : String) : IO Unit := do
  let reader_opt ← ParquetReader.open path
  match reader_opt with
  | some reader => do
    let metadata_opt ← reader.getMetadata
    match metadata_opt with
    | some m => do
      Zlog.info s!"Rows: {m.num_rows}"
      Zlog.info s!"Row groups: {m.num_row_groups}"
      Zlog.info s!"File size: {m.file_size} bytes"
    | none =>
      Zlog.warn "No metadata available"
    reader.close
  | none =>
    Zlog.error "Failed to open file"
```

## Writing Parquet Files

### Basic Writing

```lean
def writeParquet (path : String) (schema : ArrowSchema) (data : ArrowArrayStream) : IO Bool := do
  let writer_opt ← ParquetWriter.open path schema
  match writer_opt with
  | none =>
    Zlog.error s!"Failed to create writer for: {path}"
    return false
  | some writer => do
    writer.setCompression ParquetCompression.snappy
    let success ← writer.writeTable data
    writer.close
    if success then
      Zlog.info s!"Wrote: {path}"
    else
      Zlog.error "Write failed"
    return success
```

### Batch Writing

For large datasets, write in batches:

```lean
def writeBatches (path : String) (schema : ArrowSchema) (batches : Array ArrowArray) : IO Bool := do
  let writer_opt ← ParquetWriter.open path schema
  match writer_opt with
  | none => return false
  | some writer => do
    writer.setCompression ParquetCompression.zstd
    for batch in batches do
      let success ← writer.writeBatch batch
      if !success then
        Zlog.error "Batch write failed"
        writer.close
        return false
    writer.close
    Zlog.info s!"Wrote {batches.size} batches"
    return true
```

## Compression Options

| Compression | Use Case |
|-------------|----------|
| `uncompressed` | Maximum read speed, larger files |
| `snappy` | Good balance of speed and compression (default) |
| `gzip` | Better compression, slower |
| `lz4` | Fast compression and decompression |
| `zstd` | Best compression ratio |
| `brotli` | High compression, slower |

```lean
-- Set compression before writing
writer.setCompression ParquetCompression.zstd
```

## API Reference

### ParquetReader

```lean
ParquetReader.open        : String → IO (Option ParquetReader)
ParquetReader.readTable   : ParquetReader → IO (Option ArrowArrayStream)
ParquetReader.readColumns : ParquetReader → Array String → IO (Option ArrowArrayStream)
ParquetReader.readRowGroup: ParquetReader → UInt32 → IO (Option ArrowArrayStream)
ParquetReader.getMetadata : ParquetReader → IO (Option ParquetFileMetadata)
ParquetReader.close       : ParquetReader → IO Unit
```

### ParquetWriter

```lean
ParquetWriter.open          : String → ArrowSchema → IO (Option ParquetWriter)
ParquetWriter.writeTable    : ParquetWriter → ArrowArrayStream → IO Bool
ParquetWriter.writeBatch    : ParquetWriter → ArrowArray → IO Bool
ParquetWriter.setCompression: ParquetWriter → ParquetCompression → IO Unit
ParquetWriter.close         : ParquetWriter → IO Unit
```

### Metadata Types

```lean
structure ParquetFileMetadata where
  num_rows       : UInt64
  num_row_groups : UInt32
  file_size      : UInt64

structure ParquetRowGroupMetadata where
  num_rows    : UInt64
  num_columns : UInt32
  total_size  : UInt64

structure ParquetColumnMetadata where
  name        : String
  compression : ParquetCompression
```

## Error Handling

All operations return `Option` types for safe error handling:

```lean
-- Pattern: always handle None case
match ← ParquetReader.open path with
| none => Zlog.error "Operation failed"
| some reader => do
  -- Success path
  reader.close
```

## Memory Management

- Call `.close()` on readers and writers when done
- Arrow arrays from Parquet have the same lifecycle as regular Arrow arrays
- Use row group reading for memory-constrained environments

## Performance Tips

1. **Column pruning**: Only read columns you need
2. **Row group processing**: Process large files in chunks
3. **Compression choice**: Use `snappy` for speed, `zstd` for size
4. **Predicate pushdown**: Filter early when possible

## Implementation Status

| Feature | Status |
|---------|--------|
| API structure | Complete |
| C interface | Complete |
| Lean FFI | Complete |
| libparquet integration | Stub |

The current implementation provides the complete API surface. Full functionality requires linking against the Apache Arrow C++ libraries.
