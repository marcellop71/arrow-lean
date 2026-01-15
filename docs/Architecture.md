# Arrow-Lean Architecture

This document describes the architecture of arrow-lean, a pure C implementation of Apache Arrow for Lean 4.

## Overview

Arrow-lean provides a complete implementation of the Arrow columnar memory format and Parquet file writing without any C++ dependencies. This design choice offers several benefits:

- **Simplified Build**: No C++ toolchain required, just a C99 compiler
- **Reduced Binary Size**: No heavy C++ runtime or Apache Arrow C++ library
- **Easier Deployment**: Fewer system dependencies
- **Better Portability**: Pure C compiles on more platforms

## Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                    Lean Application                      │
├─────────────────────────────────────────────────────────┤
│                 ToArrow Typeclasses                      │
│           (ToArrowColumn, ToArrowBatch)                  │
├─────────────────────────────────────────────────────────┤
│                 TypedBuilders API                        │
│    (Int64Builder, StringBuilder, SchemaBuilder, etc.)   │
├─────────────────────────────────────────────────────────┤
│                    BuilderFFI                            │
│              (Lean FFI Declarations)                     │
├─────────────────────────────────────────────────────────┤
│                 lean_builder_wrapper.c                   │
│               (Lean-to-C FFI Bridge)                     │
├─────────────────────────────────────────────────────────┤
│                   arrow_builders.c                       │
│           (Pure C Arrow Array Builders)                  │
├─────────────────────────────────────────────────────────┤
│              parquet_writer_impl.c                       │
│    (Pure C Parquet Writer with Thrift Serialization)    │
├─────────────────────────────────────────────────────────┤
│                 Arrow C Data Interface                   │
│           (ArrowSchema, ArrowArray, etc.)               │
└─────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Arrow C Data Interface (`arrow/arrow_c_abi.h`)

The foundation is the Arrow C Data Interface - a minimal, stable ABI for sharing Arrow data between systems. Key structures:

```c
struct ArrowSchema {
    const char* format;      // Type format string (e.g., "l" for int64)
    const char* name;        // Field name
    const char* metadata;    // Custom metadata (key-value pairs)
    int64_t flags;           // ARROW_FLAG_NULLABLE, etc.
    int64_t n_children;      // Number of child schemas
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;          // Number of elements
    int64_t null_count;      // Number of null values
    int64_t offset;          // Starting offset in buffers
    int64_t n_buffers;       // Number of data buffers
    int64_t n_children;      // Number of child arrays
    const void** buffers;    // Data buffers (validity bitmap, values, offsets)
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};
```

### 2. Typed Builders (`arrow/arrow_builders.c`)

Type-safe builders for constructing Arrow arrays:

- **Int64Builder**: Build arrays of 64-bit integers
- **Float64Builder**: Build arrays of double-precision floats
- **StringBuilder**: Build variable-length string arrays
- **BoolBuilder**: Build boolean arrays with bit-packed storage
- **TimestampBuilder**: Build timestamp arrays with timezone support
- **SchemaBuilder**: Construct multi-column schemas
- **RecordBatch**: Bundle schema with column arrays

Each builder manages:
- Dynamic memory allocation with automatic resizing
- Validity bitmaps for nullable values
- Proper Arrow buffer layout

### 3. Parquet Writer (`arrow/parquet_writer_impl.c`)

A complete pure C Parquet writer implementing:

- **Thrift Compact Protocol**: Serialization for Parquet metadata
- **Plain Encoding**: Data encoding for all supported types
- **RLE Encoding**: For definition levels (nullable columns)
- **Page Structure**: Data pages with headers
- **Footer Serialization**: FileMetaData with schema and row groups

Supported Parquet types:
- BOOLEAN
- INT32, INT64
- FLOAT, DOUBLE
- BYTE_ARRAY (strings)
- Converted types: UTF8, TIMESTAMP_MILLIS, TIMESTAMP_MICROS

### 4. Lean FFI Layer

#### BuilderFFI.lean
Declares opaque types and external functions:

```lean
opaque Int64BuilderPointed : NonemptyType
def Int64Builder := Int64BuilderPointed.type

@[extern "lean_int64_builder_create"]
opaque Int64Builder.create : USize → IO (Option Int64Builder)

@[extern "lean_int64_builder_append"]
opaque Int64Builder.append : Int64Builder → Int64 → IO Bool
```

#### TypedBuilders.lean
High-level API wrapping the FFI:

```lean
def Int64Builder.appendOption (b : Int64Builder) (val : Option Int64) : IO Bool :=
  match val with
  | some v => b.append v
  | none => b.appendNull

def buildInt64Column (values : Array Int64) : IO (Option ArrowArray) := do
  match ← Int64Builder.create values.size.toUSize with
  | some builder =>
    for v in values do
      let _ ← builder.append v
    builder.finish
  | none => return none
```

#### ToArrow.lean
Typeclass-based serialization for custom types:

```lean
class ToArrowColumn (α : Type) where
  arrowFormat : String
  toColumn : Array α → IO (Option ArrowArray)

class ToArrowBatch (α : Type) where
  columnSpecs : Array ColumnSpec
  buildColumns : Array α → IO (Option (Array ArrowArray))
```

## Data Flow

### Creating Arrow Data from Lean Types

```
Lean Type (e.g., Trade)
    ↓
ToArrowBatch.buildColumns
    ↓
TypedBuilders (Int64Builder, StringBuilder, etc.)
    ↓
lean_builder_wrapper.c (FFI)
    ↓
arrow_builders.c (Pure C)
    ↓
ArrowArray (C Data Interface)
```

### Writing to Parquet

```
Array of Records
    ↓
ToArrowBatch.buildColumns
    ↓
RecordBatch (schema + columns)
    ↓
ArrowArrayStream
    ↓
ParquetWriter.writeTable
    ↓
parquet_reader_writer.c
    ↓
parquet_writer_impl.c
    ↓
Thrift Serialization + Data Pages
    ↓
.parquet file
```

## Memory Management

### Builder Lifecycle

1. **Create**: Allocate builder with initial capacity
2. **Append**: Add values (automatic resize if needed)
3. **Finish**: Convert to ArrowArray, builder becomes invalid
4. **Free**: Release builder resources (automatic on finish)

### Arrow Array Lifecycle

1. **Creation**: Via builder.finish() or external source
2. **Usage**: Access buffers, child arrays
3. **Release**: Call array.release() to free buffers

### Lean Integration

Lean's automatic memory management works with Arrow through:
- Opaque pointer types for builders
- Explicit release calls for Arrow structures
- RecordBatch bundles schema and array for RAII-like management

## File Organization

```
arrow/
├── arrow_c_abi.h           # Arrow C Data Interface definitions
├── arrow_schema.c          # Schema creation and management
├── arrow_array.c           # Array creation and management
├── arrow_stream.c          # Stream implementation
├── arrow_data_access.c     # Type-specific value extraction
├── arrow_buffer.c          # Buffer allocation and management
├── arrow_builders.h        # Builder declarations
├── arrow_builders.c        # Builder implementations
├── arrow_ipc.c             # IPC serialization
├── parquet_writer_impl.h   # Parquet writer declarations
├── parquet_writer_impl.c   # Parquet writer implementation
├── parquet_reader_writer.c # High-level Parquet API
├── lean_arrow_wrapper.c    # Lean FFI for core Arrow
├── lean_builder_wrapper.c  # Lean FFI for builders
├── lean_arrow_ipc.c        # Lean FFI for IPC
└── lean_parquet_wrapper.c  # Lean FFI for Parquet
```

## Build System

The lakefile.lean compiles all C sources:

```lean
extern_lib libarrow_wrapper pkg := do
  let schemaObj ← arrow_schema_o.fetch
  let arrayObj ← arrow_array_o.fetch
  let buildersObj ← arrow_builders_o.fetch
  let parquetWriterImplObj ← parquet_writer_impl_o.fetch
  -- ... etc
  buildStaticLib (pkg.staticLibDir / nameToStaticLib "arrow_wrapper")
    #[schemaObj, arrayObj, buildersObj, parquetWriterImplObj, ...]
```

Compiler flags: `-fPIC -O2 -std=c99`

## Extensibility

### Adding New Types

1. Add builder in `arrow_builders.c`:
   ```c
   typedef struct { ... } NewTypeBuilder;
   NewTypeBuilder* new_type_builder_create(size_t);
   int new_type_builder_append(NewTypeBuilder*, NewType);
   struct ArrowArray* new_type_builder_finish(NewTypeBuilder*);
   ```

2. Add FFI wrapper in `lean_builder_wrapper.c`:
   ```c
   LEAN_EXPORT lean_obj_res lean_new_type_builder_create(...);
   ```

3. Add Lean declarations in `BuilderFFI.lean`:
   ```lean
   opaque NewTypeBuilderPointed : NonemptyType
   @[extern "lean_new_type_builder_create"]
   opaque NewTypeBuilder.create : ...
   ```

4. Add high-level API in `TypedBuilders.lean`:
   ```lean
   def buildNewTypeColumn (values : Array NewType) : IO (Option ArrowArray) := ...
   ```

5. Add ToArrowColumn instance in `ToArrow.lean`:
   ```lean
   instance : ToArrowColumn NewType where
     arrowFormat := "..."
     toColumn := buildNewTypeColumn
   ```

### Adding Parquet Features

The Parquet writer can be extended with:
- Dictionary encoding for repeated strings
- Delta encoding for integers
- Compression codecs (snappy, gzip, zstd)
- Statistics (min/max values)
- Bloom filters
- Nested types (lists, maps)

## Performance Considerations

1. **Batch Operations**: Process data in batches (RecordBatch) for efficiency
2. **Zero-Copy**: Arrow C Data Interface enables zero-copy data sharing
3. **Memory Locality**: Columnar layout is cache-friendly for analytics
4. **Streaming**: ArrowArrayStream supports processing data incrementally
