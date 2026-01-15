# Apache Arrow Memory Format

[Apache Arrow](https://arrow.apache.org/) defines a language-independent columnar memory format for flat and hierarchical data. This document explains the format and its benefits for Lean applications.

## What is Arrow?

Arrow is a **columnar in-memory data format** designed for efficient analytical operations. Unlike row-oriented formats (JSON, CSV, typical database rows), Arrow stores data column-by-column.

### Row vs Columnar Layout

**Row-oriented** (traditional):
```
Record 1: [timestamp, symbol, price, volume]
Record 2: [timestamp, symbol, price, volume]
Record 3: [timestamp, symbol, price, volume]
```

**Columnar** (Arrow):
```
timestamps: [t1, t2, t3, ...]
symbols:    [s1, s2, s3, ...]
prices:     [p1, p2, p3, ...]
volumes:    [v1, v2, v3, ...]
```

## Benefits for Lean

### 1. Cache-Efficient Analytics

Columnar layout means analytical queries touch only relevant data:

```lean
-- Computing average price only reads the price column
-- No need to load timestamps, symbols, or volumes
def averagePrice (prices : ArrowArray) : IO Float := do
  let mut sum := 0.0
  let mut count := 0
  prices.forEachFloat64 fun _ value => do
    match value with
    | some v => do
      sum := sum + v
      count := count + 1
    | none => pure ()
  return if count > 0 then sum / count.toFloat else 0.0
```

With row-oriented data, computing an average requires loading entire records even though only one field is needed. Arrow's columnar layout keeps relevant data contiguous in memory, maximizing CPU cache utilization.

### 2. SIMD Vectorization

Contiguous arrays of primitive types enable SIMD (Single Instruction Multiple Data) operations. Modern CPUs can process 4-8 values simultaneously:

```
Traditional loop:    price[0] + price[1] + price[2] + price[3]  (4 operations)
SIMD:               SIMD_ADD(price[0:3])                        (1 operation)
```

Arrow's memory layout is specifically designed to enable these optimizations in the underlying C implementation.

### 3. Zero-Copy Interoperability

Arrow's standardized format enables zero-copy data sharing:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Python    │     │    Lean     │     │    Rust     │
│   (pandas)  │────▶│  (arrow-    │────▶│   (arrow-   │
│             │     │    lean)    │     │     rs)     │
└─────────────┘     └─────────────┘     └─────────────┘
        │                  │                   │
        └──────────────────┴───────────────────┘
                    Same memory buffer
                    (no serialization)
```

This means:
- Load data from Python pandas into Lean without copying
- Pass results to Rust services without serialization
- Share memory across processes using memory-mapped files

### 4. Type-Safe Null Handling

Arrow has first-class support for nullable data using validity bitmaps:

```lean
-- Arrow arrays return Option types for nullable values
def processValues (array : ArrowArray) : IO Unit := do
  for i in [:array.length.toNat] do
    match ← array.getFloat64 i.toUSize with
    | some value => Zlog.debug s!"Value: {value}"
    | none => Zlog.debug "NULL"
```

The null bitmap is separate from the data, so:
- Non-null data is contiguous (cache-friendly)
- Null checks are branch-predictable
- Memory overhead is minimal (1 bit per value)

### 5. Efficient String Handling

Arrow uses offset-based string storage:

```
Offsets: [0, 5, 10, 14]
Data:    "HelloWorldTest"

String 0: data[0:5]   = "Hello"
String 1: data[5:10]  = "World"
String 2: data[10:14] = "Test"
```

Benefits:
- No per-string allocation overhead
- Contiguous memory for the entire column
- O(1) random access to any string

## Arrow Data Model

### Primitive Types

| Lean Type | Arrow Format | Description |
|-----------|--------------|-------------|
| `Bool` | `b` | Boolean |
| `Int8`..`Int64` | `c`, `s`, `i`, `l` | Signed integers |
| `UInt8`..`UInt64` | `C`, `S`, `I`, `L` | Unsigned integers |
| `Float` | `f`, `g` | Float32, Float64 |
| `String` | `u` | UTF-8 strings |

### Temporal Types

```lean
-- Timestamps with various precisions
ArrowType.timestamp TimeUnit.nanosecond   -- High-frequency trading
ArrowType.timestamp TimeUnit.millisecond  -- General timestamps
ArrowType.date32                          -- Calendar dates
```

### Nested Types

```lean
-- List of integers
ArrowType.list ArrowType.int64

-- Struct (record type)
ArrowType.struct #[
  ("timestamp", ArrowType.timestamp TimeUnit.nanosecond),
  ("symbol", ArrowType.string),
  ("price", ArrowType.float64),
  ("volume", ArrowType.uint64)
]
```

## Memory Layout

### Array Structure

An Arrow array consists of:

```
ArrowArray
├── length      : number of elements
├── null_count  : number of null values
├── offset      : starting position (for slicing)
├── buffers
│   ├── validity bitmap (1 bit per value)
│   └── data buffer (contiguous values)
└── children    : nested arrays (for lists/structs)
```

### Buffer Alignment

Arrow requires 64-byte alignment for buffers, enabling:
- Optimal SIMD instruction use
- Efficient memory-mapped I/O
- Cross-platform consistency

## Use Cases

### Financial Market Data

Arrow is ideal for time-series financial data:

```lean
def tradeSchema : ArrowType :=
  ArrowType.struct #[
    ("timestamp", ArrowType.timestamp TimeUnit.nanosecond),
    ("symbol", ArrowType.string),
    ("price", ArrowType.float64),
    ("size", ArrowType.uint64),
    ("exchange", ArrowType.string)
  ]
```

Benefits:
- Nanosecond timestamps for HFT
- Efficient aggregations (VWAP, OHLC)
- Zero-copy from market data feeds

### Data Pipelines

Arrow serves as the interchange format:

```
Data Source → Arrow → Transform → Arrow → Analytics
     │                    │                   │
  (Parquet)            (Lean)             (Python)
```

### Machine Learning

Arrow integrates with ML frameworks:
- Feature extraction in Lean
- Training in Python (via PyArrow)
- Inference back in Lean
- No serialization overhead

## Performance Characteristics

| Operation | Row-Oriented | Arrow Columnar |
|-----------|--------------|----------------|
| Read single column | O(n * row_size) | O(n * type_size) |
| Aggregate column | Poor cache use | Optimal cache use |
| Add new record | O(1) | O(columns) |
| Add new column | O(n * row_size) | O(1) |
| Random row access | O(1) | O(columns) |

Arrow excels at:
- Analytical queries (aggregations, filters)
- Columnar operations (transforms)
- Batch processing

Row-oriented is better for:
- Single-record CRUD operations
- Frequent row-level updates
- OLTP workloads

## Arrow-Lean Architecture

```
┌──────────────────────────────────────┐
│           Lean Application           │
├──────────────────────────────────────┤
│  ArrowLean (Type-safe Lean API)      │
│  - ArrowArray, ArrowSchema           │
│  - Type conversions                  │
│  - Iterators                         │
├──────────────────────────────────────┤
│  FFI Layer (Lean ↔ C)               │
│  - lean_arrow_wrapper.c              │
│  - Memory management                 │
├──────────────────────────────────────┤
│  Arrow C Data Interface              │
│  - arrow_c_abi.h                     │
│  - Zero-copy semantics               │
└──────────────────────────────────────┘
```

The Arrow C Data Interface enables interoperability without requiring the full Arrow C++ library for basic operations.

## Getting Started

```lean
import ArrowLean

-- Create a schema for your data
def mySchema := ArrowType.struct #[
  ("id", ArrowType.int64),
  ("name", ArrowType.string),
  ("value", ArrowType.float64)
]

-- Process Arrow data
def processData (stream : ArrowArrayStream) : IO Unit := do
  stream.forEachArray fun array => do
    Zlog.info s!"Processing {array.length} records"
    array.forEachFloat64 fun idx value => do
      match value with
      | some v => Zlog.debug s!"[{idx}] = {v}"
      | none => Zlog.debug s!"[{idx}] = NULL"
```

## Further Reading

- [Apache Arrow Specification](https://arrow.apache.org/docs/format/Columnar.html)
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
- [Parquet Support](Parquet.md) - Persistent storage for Arrow data
