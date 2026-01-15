# arrow-lean

Lean 4 bindings for [Apache Arrow](https://arrow.apache.org/), the universal columnar data format. Provides type-safe access to Arrow arrays, schemas, and streams with Parquet file writing support.

> ⚠️ **Warning**: this is work in progress, it is still incomplete and it ~~may~~ will contain errors

## AI Assistance Disclosure

Parts of this repository were created with assistance from AI-powered coding tools, specifically Claude by Anthropic. Not all generated code may have been reviewed. Generated code may have been adapted by the author. Design choices, architectural decisions, and final validation were performed independently by the author.

**Warning: This is a work in progress and still unstable.**

## Overview

Arrow-Lean is a **pure C implementation** of Apache Arrow for Lean 4 - no C++ dependencies required. This provides:

- **Simplified Build**: Just a C99 compiler, no Apache Arrow C++ library needed
- **Easy Deployment**: Minimal system dependencies
- **High-performance data processing** with cache-friendly columnar layout
- **Zero-copy interoperability** with other Arrow-enabled systems (Python/pandas, Rust, Go, etc.)
- **Type-safe data access** with Lean's strong type system
- **Typed column builders** for programmatic Arrow array construction
- **Pure C Parquet writer** for persistent columnar storage
- **IPC serialization** for storage and transmission of Arrow data

Apache Arrow is a universal in-memory format for tabular ("columnar") data, enabling zero-copy sharing of Arrow data between independent runtimes and components running in the same process.

See [docs/Arrow.md](docs/Arrow.md) for details on the Arrow memory format.
See [docs/Architecture.md](docs/Architecture.md) for the implementation architecture.

## Status

| Component | Status |
|-----------|--------|
| Arrow Schema | Implemented (pure C) |
| Arrow Array | Implemented (pure C) |
| Arrow Stream | Implemented (pure C) |
| Data Access | Implemented (Int64, UInt64, Float64, Float32, Int32, String, Bool) |
| Typed Builders | Implemented (Int64, Float64, String, Bool, Timestamp) |
| Buffer Management | Implemented (pure C) |
| ArrowM Monad | Implemented |
| Typed Errors | Implemented |
| IPC Serialization | Implemented (pure C) |
| Parquet Writer | Implemented (pure C, no compression) |
| Parquet Reader | Not implemented |
| ToArrow Typeclasses | Implemented |

## Project Structure

```
arrow-lean/
├── ArrowLean/               # Main library
│   ├── Ops.lean             # Core types: ArrowSchema, ArrowArray, ArrowType
│   ├── FFI.lean             # Arrow FFI bindings
│   ├── Utils.lean           # Type conversions, iterators
│   ├── Memory.lean          # Resource management & finalization
│   ├── Error.lean           # Typed error handling
│   ├── Monad.lean           # ArrowM monad for table operations
│   ├── IPC.lean             # IPC serialization
│   ├── BuilderFFI.lean      # Typed builder FFI bindings
│   ├── TypedBuilders.lean   # High-level builder API
│   ├── ToArrow.lean         # ToArrowColumn/ToArrowBatch typeclasses
│   ├── Parquet.lean         # Parquet types and options
│   └── ParquetFFI.lean      # Parquet FFI bindings
├── ArrowLean.lean           # Module exports
├── arrow/                   # Pure C implementation
│   ├── arrow_c_abi.h        # Arrow C Data Interface structs
│   ├── arrow_builders.h     # Typed builder declarations
│   ├── arrow_builders.c     # Builder implementations
│   ├── arrow_schema.c       # Schema implementation
│   ├── arrow_array.c        # Array implementation
│   ├── arrow_stream.c       # Stream implementation
│   ├── arrow_data_access.c  # Type-specific value extraction
│   ├── arrow_buffer.c       # Buffer management
│   ├── arrow_ipc.c          # IPC serialization
│   ├── parquet_writer_impl.h  # Parquet writer declarations
│   ├── parquet_writer_impl.c  # Pure C Parquet writer
│   ├── parquet_reader_writer.c # High-level Parquet API
│   ├── lean_builder_wrapper.c  # Builder FFI exports
│   ├── lean_arrow_wrapper.c    # Core Arrow FFI exports
│   ├── lean_arrow_ipc.c        # IPC FFI exports
│   └── lean_parquet_wrapper.c  # Parquet FFI exports
├── Examples/
│   ├── Main.lean
│   ├── ParquetExample.lean
│   └── TypedBuildersExample.lean  # Typed builders & ToArrow examples
├── docs/
│   ├── Arrow.md             # Arrow format documentation
│   ├── Architecture.md      # Implementation architecture
│   └── Parquet.md           # Parquet usage guide
├── lakefile.lean
└── lean-toolchain           # Lean version (v4.27.0-rc1)
```

## Requirements

- **Lean 4**: v4.27.0-rc1 or compatible
- **C compiler**: gcc or clang with C99 support
- **zlog**: Logging library

### System Dependencies

**Ubuntu/Debian:**
```bash
sudo apt-get install libzlog-dev
```

**macOS:**
```bash
brew install zlog
```

No C++ libraries or Apache Arrow C++ installation required.

### Lean Dependencies

- `zlogLean` - Structured logging
- `Cli` - CLI argument parsing

## Building

```bash
# Build the library
lake build ArrowLean

# Run examples
lake exe examples
```

## Usage

Add to your `lakefile.lean`:

```lean
require arrowLean from git
  "https://github.com/your-org/arrow-lean" @ "main"
```

### ArrowM Monad (Recommended)

The `ArrowM` monad provides typed error handling and composable table operations:

```lean
import ArrowLean

open Arrow

-- Define operations on a table
def computeStats : ArrowM (Float × Nat) := do
  let total ← sumFloat64
  let count ← countNonNull getFloat64At
  return (total, count)

-- Run with automatic error handling
def main : IO Unit := do
  let schema ← ArrowSchema.init "g"  -- float64
  let array ← ArrowArray.init 1000

  match ← withTable schema array computeStats with
  | .ok (total, count) =>
    IO.println s!"Total: {total}, Count: {count}"
  | .error e =>
    IO.println s!"Error: {e}"

  schema.release
  array.release
```

### Typed Errors

```lean
-- Error kinds for precise error handling
inductive Arrow.ErrorKind
  | nullAccess        -- Attempted to read null value
  | typeMismatch      -- Column type doesn't match
  | indexOutOfBounds  -- Array index out of range
  | columnNotFound    -- Named column doesn't exist
  | invalidSchema     -- Schema is malformed
  | allocationFailed  -- Memory allocation failed
  | serializationFailed
  | ioError
  | other

-- Pattern match on errors
match result with
| .error { kind := .indexOutOfBounds, .. } => handleBounds
| .error { kind := .nullAccess, .. } => handleNull
| .ok value => process value
```

### ArrowM Operations

```lean
-- Row access with bounds checking
getInt64At   : Nat → ArrowM (Option Int64)
getFloat64At : Nat → ArrowM (Option Float)
getStringAt  : Nat → ArrowM (Option String)
getBoolAt    : Nat → ArrowM (Option Bool)

-- Iteration
mapRows     : (Nat → ArrowM α) → ArrowM (Array α)
forEachRow  : (Nat → ArrowM Unit) → ArrowM Unit
foldRows    : β → (β → Nat → ArrowM β) → ArrowM β

-- Filtering
filterRowIndices : (Nat → ArrowM Bool) → ArrowM (Array Nat)

-- Aggregation
sumInt64    : ArrowM Int64
sumFloat64  : ArrowM Float
avgFloat64  : ArrowM (Option Float)
countNonNull : (Nat → ArrowM (Option α)) → ArrowM Nat
```

### Direct IO Interface

For simple use cases, you can also use the direct IO functions:

```lean
import ArrowLean

def processArray (array : ArrowArray) : IO Unit := do
  for i in [:array.length.toNat] do
    let idx := i.toUSize
    match ← array.getFloat64 idx with
    | some v => Zlog.debug s!"[{idx}] = {v}"
    | none => Zlog.debug s!"[{idx}] = null"
```

### Working with Streams

```lean
def processStream (stream : ArrowArrayStream) : IO Unit := do
  stream.forEachArray fun array => do
    Zlog.info s!"Processing batch with {array.length} rows"
```

### Creating Schemas

```lean
def createSchema : IO ArrowSchema := do
  -- Primitive type schema
  let priceSchema ← ArrowSchema.forType ArrowType.float64 "price"

  -- Struct schema for tables
  let tableSchema ← ArrowSchema.struct "trades"
  return tableSchema
```

## Typed Builders (Recommended for Data Creation)

The typed builder API provides the most efficient way to create Arrow arrays from Lean data:

### Building Individual Columns

```lean
import ArrowLean

-- Build an Int64 column
let values : Array Int64 := #[100, 200, 300, 400, 500]
match ← buildInt64Column values with
| some arr =>
  IO.println s!"Created column with {arr.length} values"
  arr.release
| none => IO.println "Failed"

-- Build a String column with nulls
let optStrings : Array (Option String) := #[some "hello", none, some "world"]
match ← buildOptStringColumn optStrings with
| some arr =>
  IO.println s!"Created column with {arr.nullCount} nulls"
  arr.release
| none => IO.println "Failed"
```

### Building Multi-Column Tables

```lean
-- Create schema with SchemaBuilder
match ← SchemaBuilder.create 3 with
| some sb =>
  let _ ← sb.addInt64 "id" false        -- non-nullable
  let _ ← sb.addString "name" true      -- nullable
  let _ ← sb.addFloat64 "score" false   -- non-nullable

  match ← sb.finish with
  | some schema =>
    -- Build columns...
    let batch ← RecordBatch.create schema columns rowCount
    -- Use batch...
  | none => ...
| none => ...
```

### ToArrowBatch Typeclass (Best Practice)

Define how your custom types map to Arrow format:

```lean
-- Define a custom type
structure Trade where
  timestamp : Int64
  symbol : Option String
  price : Float
  quantity : Nat

-- Implement ToArrowBatch
instance : ToArrowBatch Trade where
  columnSpecs := #[
    ColumnSpec.timestamp "timestamp" "UTC" false,
    ColumnSpec.string "symbol" true,
    ColumnSpec.float64 "price" false,
    ColumnSpec.int64 "quantity" false
  ]

  buildColumns trades := do
    let tsCol ← buildTimestampColumn (trades.map (·.timestamp))
    let symCol ← buildOptStringColumn (trades.map (·.symbol))
    let priceCol ← buildFloat64Column (trades.map (·.price))
    let qtyCol ← buildInt64Column (trades.map (·.quantity.toInt64))

    match tsCol, symCol, priceCol, qtyCol with
    | some ts, some sym, some price, some qty =>
      return some #[ts, sym, price, qty]
    | _, _, _, _ => return none

-- Use it
let trades : Array Trade := #[...]

-- Convert to RecordBatch
match ← toRecordBatch trades with
| some batch => ...

-- Write directly to Parquet
let ok ← writeRecordsToParquet "trades.parquet" trades

-- Or write to IPC format
let ok ← writeRecordsToIPC "trades.arrow" trades

-- Serialize to bytes
match ← serializeRecordsToIPC trades with
| some bytes => -- Send bytes over network, store in Redis, etc.
| none => ...
```

## Supported Arrow Types

| Type | Format | Description |
|------|--------|-------------|
| `null` | `n` | Null values |
| `boolean` | `b` | Boolean |
| `int8`..`int64` | `c`, `s`, `i`, `l` | Signed integers |
| `uint8`..`uint64` | `C`, `S`, `I`, `L` | Unsigned integers |
| `float16`..`float64` | `e`, `f`, `g` | Floating-point |
| `string` | `u` | UTF-8 text |
| `binary` | `z` | Binary data |
| `timestamp` | `ts{unit}:UTC` | Timestamps |
| `date32`, `date64` | `tdD`, `tdm` | Date values |
| `list` | `+l` | List/array |
| `struct` | `+s` | Structured records |

## API Reference

### ArrowArray

```lean
-- Data access (Option for nullable values)
ArrowArray.getInt64   : ArrowArray → USize → IO (Option Int64)
ArrowArray.getUInt64  : ArrowArray → USize → IO (Option UInt64)
ArrowArray.getFloat64 : ArrowArray → USize → IO (Option Float)
ArrowArray.getFloat32 : ArrowArray → USize → IO (Option Float)
ArrowArray.getInt32   : ArrowArray → USize → IO (Option Int32)
ArrowArray.getString  : ArrowArray → USize → IO (Option String)
ArrowArray.getBool    : ArrowArray → USize → IO (Option Bool)

-- Iteration
ArrowArray.forEachInt64   : ArrowArray → (USize → Option Int64 → IO Unit) → IO Unit
ArrowArray.forEachFloat64 : ArrowArray → (USize → Option Float → IO Unit) → IO Unit
ArrowArray.forEachString  : ArrowArray → (USize → Option String → IO Unit) → IO Unit

-- Lifecycle
ArrowArray.init    : UInt64 → IO ArrowArray
ArrowArray.release : ArrowArray → IO Unit
```

### ArrowSchema

```lean
ArrowSchema.init    : String → IO ArrowSchema
ArrowSchema.forType : ArrowType → String → IO ArrowSchema
ArrowSchema.struct  : String → IO ArrowSchema
ArrowSchema.release : ArrowSchema → IO Unit
```

### ArrowArrayStream

```lean
ArrowArrayStream.getNext      : ArrowArrayStream → IO (Option ArrowArray)
ArrowArrayStream.getSchema    : ArrowArrayStream → IO (Option ArrowSchema)
ArrowArrayStream.forEachArray : ArrowArrayStream → (ArrowArray → IO Unit) → IO Unit
ArrowArrayStream.toArrays     : ArrowArrayStream → IO (Array ArrowArray)
```

### ArrowBuffer

```lean
ArrowBuffer.allocate : USize → IO ArrowBuffer
ArrowBuffer.resize   : ArrowBuffer → USize → IO ArrowBuffer
ArrowBuffer.free     : ArrowBuffer → IO Unit
```

## IPC Serialization

The IPC module enables serializing Arrow data to binary format for storage or transmission.

### RecordBatch

A `RecordBatch` bundles a schema with its array data:

```lean
import ArrowLean

open ArrowLean.IPC

-- Create a batch from schema and array
let batch : RecordBatch := { schema := mySchema, array := myArray }

-- Access batch properties
let rows := batch.length
let fmt := batch.format
```

### Serialization

```lean
open ArrowLean.IPC

-- Serialize a RecordBatch to ByteArray
let data ← serialize batch

-- Deserialize ByteArray back to RecordBatch
match ← deserialize data with
| some batch =>
  IO.println s!"Loaded {batch.length} rows"
| none =>
  IO.println "Deserialization failed"
```

### Schema-Only Serialization

```lean
-- Serialize just the schema
let schemaData ← serializeSchema schema

-- Deserialize schema
match ← deserializeSchema schemaData with
| some schema => IO.println s!"Schema format: {schema.format}"
| none => IO.println "Invalid schema data"
```

### IPC API Reference

```lean
-- RecordBatch type
structure RecordBatch where
  schema : ArrowSchema
  array : ArrowArray

-- Serialization functions
IPC.serialize         : RecordBatch → IO ByteArray
IPC.deserialize       : ByteArray → IO (Option RecordBatch)
IPC.serializeSchema   : ArrowSchema → IO ByteArray
IPC.deserializeSchema : ByteArray → IO (Option ArrowSchema)
IPC.serializeArray    : ArrowArray → ArrowSchema → IO ByteArray
IPC.deserializeArray  : ByteArray → ArrowSchema → IO (Option ArrowArray)
IPC.serializedSize    : RecordBatch → IO UInt64
```

## Parquet Support

See [docs/Parquet.md](docs/Parquet.md) for detailed Parquet documentation.

### Reading Parquet Files

```lean
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

### Writing Parquet Files

```lean
def writeParquet (path : String) (schema : ArrowSchema) : IO Unit := do
  let writer_opt ← ParquetWriter.open path schema
  match writer_opt with
  | some writer => do
    writer.setCompression ParquetCompression.snappy
    let array ← ArrowArray.init 100
    let _ ← writer.writeBatch array
    writer.close
    array.release
  | none =>
    Zlog.error "Failed to create writer"
```

### Compression Options

| Compression | Description |
|-------------|-------------|
| `uncompressed` | Maximum read speed |
| `snappy` | Good balance (default) |
| `gzip` | Better compression |
| `lz4` | Fast compression |
| `zstd` | Best compression ratio |
| `brotli` | High compression |

## Integration with Redis

For storing Arrow data in Redis, see [redis-lean](https://github.com/marcellop71/redis-lean) which provides the `RedisArrow` module with:
- **Table Storage**: Store Arrow schemas and batches in Redis keys
- **Stream Micro-Batching**: Convert Redis Streams to Arrow RecordBatches

## Documentation

- [Arrow Memory Format](docs/Arrow.md) - Understanding Arrow's columnar layout
- [Parquet Support](docs/Parquet.md) - Reading and writing Parquet files
