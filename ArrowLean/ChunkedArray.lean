/-
  ArrowLean.ChunkedArray - ChunkedArray and Table types

  ChunkedArray represents a column as a sequence of contiguous chunks.
  Table represents a table as columns of ChunkedArrays.
-/

import ArrowLean.Ops
import ArrowLean.FFI

namespace ArrowLean

-- ============================================================================
-- Opaque pointer types
-- ============================================================================

/-- Opaque pointer to a ChunkedArray -/
opaque ChunkedArrayPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩

/-- Opaque pointer to a Table -/
opaque TablePtr : NonemptyType := ⟨Unit, ⟨()⟩⟩

instance : Nonempty ChunkedArrayPtr.type := ChunkedArrayPtr.property
instance : Nonempty TablePtr.type := TablePtr.property

-- ============================================================================
-- ChunkedArray FFI declarations
-- ============================================================================

@[extern "lean_chunked_array_create"]
opaque chunked_array_create_impl : @& ArrowSchemaPtr.type → IO (Option ChunkedArrayPtr.type)

@[extern "lean_chunked_array_from_array"]
opaque chunked_array_from_array_impl : @& ArrowArrayPtr.type → @& ArrowSchemaPtr.type → IO (Option ChunkedArrayPtr.type)

@[extern "lean_chunked_array_add_chunk"]
opaque chunked_array_add_chunk_impl : ChunkedArrayPtr.type → @& ArrowArrayPtr.type → IO Bool

@[extern "lean_chunked_array_get_chunk"]
opaque chunked_array_get_chunk_impl : @& ChunkedArrayPtr.type → UInt64 → IO (Option ArrowArrayPtr.type)

@[extern "lean_chunked_array_num_chunks"]
opaque chunked_array_num_chunks_impl : @& ChunkedArrayPtr.type → IO UInt64

@[extern "lean_chunked_array_length"]
opaque chunked_array_length_impl : @& ChunkedArrayPtr.type → IO UInt64

@[extern "lean_chunked_array_null_count"]
opaque chunked_array_null_count_impl : @& ChunkedArrayPtr.type → IO UInt64

@[extern "lean_chunked_array_type"]
opaque chunked_array_type_impl : @& ChunkedArrayPtr.type → IO (Option ArrowSchemaPtr.type)

@[extern "lean_chunked_array_slice"]
opaque chunked_array_slice_impl : ChunkedArrayPtr.type → Int64 → Int64 → IO (Option ChunkedArrayPtr.type)

-- ============================================================================
-- Table FFI declarations
-- ============================================================================

@[extern "lean_table_create"]
opaque table_create_impl : @& ArrowSchemaPtr.type → IO (Option TablePtr.type)

@[extern "lean_table_from_record_batch"]
opaque table_from_record_batch_impl : @& ArrowArrayPtr.type → @& ArrowSchemaPtr.type → IO (Option TablePtr.type)

@[extern "lean_table_get_column"]
opaque table_get_column_impl : @& TablePtr.type → UInt64 → IO (Option ChunkedArrayPtr.type)

@[extern "lean_table_get_column_by_name"]
opaque table_get_column_by_name_impl : @& TablePtr.type → @& String → IO (Option ChunkedArrayPtr.type)

@[extern "lean_table_num_columns"]
opaque table_num_columns_impl : @& TablePtr.type → IO UInt64

@[extern "lean_table_num_rows"]
opaque table_num_rows_impl : @& TablePtr.type → IO UInt64

@[extern "lean_table_schema"]
opaque table_schema_impl : @& TablePtr.type → IO (Option ArrowSchemaPtr.type)

@[extern "lean_table_column_name"]
opaque table_column_name_impl : @& TablePtr.type → UInt64 → IO (Option String)

@[extern "lean_table_slice"]
opaque table_slice_impl : TablePtr.type → Int64 → Int64 → IO (Option TablePtr.type)

-- ============================================================================
-- ChunkedArray high-level API
-- ============================================================================

/-- A ChunkedArray represents a column as a sequence of chunks -/
structure ChunkedArray where
  ptr : ChunkedArrayPtr.type
  deriving Nonempty

namespace ChunkedArray

/-- Create an empty ChunkedArray with the given schema -/
def create (schema : ArrowSchema) : IO (Option ChunkedArray) := do
  let opt ← chunked_array_create_impl schema.ptr
  return opt.map fun ptr => { ptr := ptr }

/-- Create a ChunkedArray from a single array -/
def fromArray (array : ArrowArray) (schema : ArrowSchema) : IO (Option ChunkedArray) := do
  let opt ← chunked_array_from_array_impl array.ptr schema.ptr
  return opt.map fun ptr => { ptr := ptr }

/-- Add a chunk to the ChunkedArray -/
def addChunk (ca : ChunkedArray) (chunk : ArrowArray) : IO Bool :=
  chunked_array_add_chunk_impl ca.ptr chunk.ptr

/-- Get a chunk by index -/
def getChunk (ca : ChunkedArray) (index : UInt64) : IO (Option ArrowArray) := do
  let opt ← chunked_array_get_chunk_impl ca.ptr index
  match opt with
  | none => return none
  | some ptr => do
    let length ← arrow_array_get_length_impl ptr
    let null_count ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr := ptr, length := length, null_count := null_count, offset := offset }

/-- Get the number of chunks -/
def numChunks (ca : ChunkedArray) : IO UInt64 :=
  chunked_array_num_chunks_impl ca.ptr

/-- Get the total length across all chunks -/
def length (ca : ChunkedArray) : IO UInt64 :=
  chunked_array_length_impl ca.ptr

/-- Get the total null count across all chunks -/
def nullCount (ca : ChunkedArray) : IO UInt64 :=
  chunked_array_null_count_impl ca.ptr

/-- Get the type schema -/
def getType (ca : ChunkedArray) : IO (Option ArrowSchema) := do
  let opt ← chunked_array_type_impl ca.ptr
  match opt with
  | none => return none
  | some ptr => do
    let format ← arrow_schema_get_format_impl ptr
    let name ← arrow_schema_get_name_impl ptr
    let flags ← arrow_schema_get_flags_impl ptr
    return some { ptr := ptr, format := format, name := name, flags := flags }

/-- Slice a ChunkedArray -/
def slice (ca : ChunkedArray) (offset : Int64) (length : Int64) : IO (Option ChunkedArray) := do
  let opt ← chunked_array_slice_impl ca.ptr offset length
  return opt.map fun ptr => { ptr := ptr }

/-- Get all chunks as an array -/
def getChunks (ca : ChunkedArray) : IO (Array ArrowArray) := do
  let count ← ca.numChunks
  let mut chunks := #[]
  for i in [0:count.toNat] do
    if let some chunk ← ca.getChunk i.toUInt64 then
      chunks := chunks.push chunk
  return chunks

end ChunkedArray

-- ============================================================================
-- Table high-level API
-- ============================================================================

/-- A Table represents a table as columns of ChunkedArrays -/
structure Table where
  ptr : TablePtr.type
  deriving Nonempty

namespace Table

/-- Create an empty Table with the given schema -/
def create (schema : ArrowSchema) : IO (Option Table) := do
  let opt ← table_create_impl schema.ptr
  return opt.map fun ptr => { ptr := ptr }

/-- Create a Table from a record batch -/
def fromRecordBatch (batch : ArrowArray) (schema : ArrowSchema) : IO (Option Table) := do
  let opt ← table_from_record_batch_impl batch.ptr schema.ptr
  return opt.map fun ptr => { ptr := ptr }

/-- Get a column by index -/
def getColumn (table : Table) (index : UInt64) : IO (Option ChunkedArray) := do
  let opt ← table_get_column_impl table.ptr index
  return opt.map fun ptr => { ptr := ptr }

/-- Get a column by name -/
def getColumnByName (table : Table) (name : String) : IO (Option ChunkedArray) := do
  let opt ← table_get_column_by_name_impl table.ptr name
  return opt.map fun ptr => { ptr := ptr }

/-- Get the number of columns -/
def numColumns (table : Table) : IO UInt64 :=
  table_num_columns_impl table.ptr

/-- Get the number of rows -/
def numRows (table : Table) : IO UInt64 :=
  table_num_rows_impl table.ptr

/-- Get the table schema -/
def getSchema (table : Table) : IO (Option ArrowSchema) := do
  let opt ← table_schema_impl table.ptr
  match opt with
  | none => return none
  | some ptr => do
    let format ← arrow_schema_get_format_impl ptr
    let name ← arrow_schema_get_name_impl ptr
    let flags ← arrow_schema_get_flags_impl ptr
    return some { ptr := ptr, format := format, name := name, flags := flags }

/-- Get a column name by index -/
def columnName (table : Table) (index : UInt64) : IO (Option String) :=
  table_column_name_impl table.ptr index

/-- Slice a Table (select rows) -/
def slice (table : Table) (offset : Int64) (length : Int64) : IO (Option Table) := do
  let opt ← table_slice_impl table.ptr offset length
  return opt.map fun ptr => { ptr := ptr }

/-- Get all columns as an array -/
def getColumns (table : Table) : IO (Array ChunkedArray) := do
  let count ← table.numColumns
  let mut columns := #[]
  for i in [0:count.toNat] do
    if let some col ← table.getColumn i.toUInt64 then
      columns := columns.push col
  return columns

/-- Get all column names -/
def columnNames (table : Table) : IO (Array String) := do
  let count ← table.numColumns
  let mut names := #[]
  for i in [0:count.toNat] do
    if let some name ← table.columnName i.toUInt64 then
      names := names.push name
  return names

end Table

end ArrowLean
