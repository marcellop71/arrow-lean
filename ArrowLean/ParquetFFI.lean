-- Parquet FFI bindings for Arrow-Lean
-- Low-level C bindings for libparquet operations

import ArrowLean.Parquet

-- External Parquet Reader C functions
@[extern "lean_parquet_reader_open"]
opaque parquet_reader_open_impl (file_path: @& String) : IO (Option ParquetReaderPtr.type)

@[extern "lean_parquet_reader_close"]
opaque parquet_reader_close_impl (reader: @& ParquetReaderPtr.type) : IO Unit

@[extern "lean_parquet_reader_get_metadata"]
opaque parquet_reader_get_metadata_impl (reader: @& ParquetReaderPtr.type) : IO (Option ParquetFileMetadataPtr.type)

@[extern "lean_parquet_reader_read_table"]
opaque parquet_reader_read_table_impl (reader: @& ParquetReaderPtr.type) : IO (Option ArrowArrayStreamPtr.type)

@[extern "lean_parquet_reader_read_row_group"]
opaque parquet_reader_read_row_group_impl (reader: @& ParquetReaderPtr.type) (row_group: UInt32) : IO (Option ArrowArrayStreamPtr.type)

@[extern "lean_parquet_reader_read_columns"]
opaque parquet_reader_read_columns_impl (reader: @& ParquetReaderPtr.type) (columns: @& Array String) : IO (Option ArrowArrayStreamPtr.type)

-- External Parquet Writer C functions
@[extern "lean_parquet_writer_open"]
opaque parquet_writer_open_impl (file_path: @& String) (schema: @& ArrowSchemaPtr.type) : IO (Option ParquetWriterPtr.type)

@[extern "lean_parquet_writer_close"]
opaque parquet_writer_close_impl (writer: @& ParquetWriterPtr.type) : IO Unit

@[extern "lean_parquet_writer_write_table"]
opaque parquet_writer_write_table_impl (writer: @& ParquetWriterPtr.type) (stream: @& ArrowArrayStreamPtr.type) : IO Bool

@[extern "lean_parquet_writer_write_batch"]
opaque parquet_writer_write_batch_impl (writer: @& ParquetWriterPtr.type) (array: @& ArrowArrayPtr.type) : IO Bool

@[extern "lean_parquet_writer_set_compression"]
opaque parquet_writer_set_compression_impl (writer: @& ParquetWriterPtr.type) (compression: UInt32) : IO Unit

-- External Parquet Metadata C functions
@[extern "lean_parquet_metadata_get_num_rows"]
opaque parquet_metadata_get_num_rows_impl (metadata: @& ParquetFileMetadataPtr.type) : IO UInt64

@[extern "lean_parquet_metadata_get_num_row_groups"]
opaque parquet_metadata_get_num_row_groups_impl (metadata: @& ParquetFileMetadataPtr.type) : IO UInt32

@[extern "lean_parquet_metadata_get_file_size"]
opaque parquet_metadata_get_file_size_impl (metadata: @& ParquetFileMetadataPtr.type) : IO UInt64

@[extern "lean_parquet_metadata_get_row_group"]
opaque parquet_metadata_get_row_group_impl (metadata: @& ParquetFileMetadataPtr.type) (index: UInt32) : IO (Option ParquetRowGroupMetadataPtr.type)

@[extern "lean_parquet_row_group_get_num_rows"]
opaque parquet_row_group_get_num_rows_impl (row_group: @& ParquetRowGroupMetadataPtr.type) : IO UInt64

@[extern "lean_parquet_row_group_get_num_columns"]
opaque parquet_row_group_get_num_columns_impl (row_group: @& ParquetRowGroupMetadataPtr.type) : IO UInt32

@[extern "lean_parquet_row_group_get_total_byte_size"]
opaque parquet_row_group_get_total_byte_size_impl (row_group: @& ParquetRowGroupMetadataPtr.type) : IO UInt64

-- Helper function to convert compression enum to UInt32
def compressionToUInt32 (compression: ParquetCompression) : UInt32 :=
  match compression with
  | ParquetCompression.uncompressed => 0
  | ParquetCompression.snappy => 1
  | ParquetCompression.gzip => 2
  | ParquetCompression.lzo => 3
  | ParquetCompression.brotli => 4
  | ParquetCompression.lz4 => 5
  | ParquetCompression.zstd => 6

-- High-level Parquet Reader Operations
def ParquetReader.open (file_path: String) : IO (Option ParquetReader) := do
  let opt_ptr ← parquet_reader_open_impl file_path
  match opt_ptr with
  | none => return none
  | some ptr =>
    return some { ptr := ptr, file_path := file_path, metadata := none }

def ParquetReader.close (reader: ParquetReader) : IO Unit :=
  parquet_reader_close_impl reader.ptr

def ParquetReader.getMetadata (reader: ParquetReader) : IO (Option ParquetFileMetadata) := do
  let opt_ptr ← parquet_reader_get_metadata_impl reader.ptr
  match opt_ptr with
  | none => return none
  | some ptr => do
    let num_rows ← parquet_metadata_get_num_rows_impl ptr
    let num_row_groups ← parquet_metadata_get_num_row_groups_impl ptr
    let file_size ← parquet_metadata_get_file_size_impl ptr
    return some { ptr := ptr, num_rows := num_rows, num_row_groups := num_row_groups, file_size := file_size }

def ParquetReader.readTable (reader: ParquetReader) : IO (Option ArrowArrayStream) := do
  let opt_ptr ← parquet_reader_read_table_impl reader.ptr
  match opt_ptr with
  | none => return none
  | some ptr => return some { ptr := ptr }

def ParquetReader.readRowGroup (reader: ParquetReader) (row_group: UInt32) : IO (Option ArrowArrayStream) := do
  let opt_ptr ← parquet_reader_read_row_group_impl reader.ptr row_group
  match opt_ptr with
  | none => return none
  | some ptr => return some { ptr := ptr }

def ParquetReader.readColumns (reader: ParquetReader) (columns: Array String) : IO (Option ArrowArrayStream) := do
  let opt_ptr ← parquet_reader_read_columns_impl reader.ptr columns
  match opt_ptr with
  | none => return none
  | some ptr => return some { ptr := ptr }

-- High-level Parquet Writer Operations
def ParquetWriter.open (file_path: String) (schema: ArrowSchema) : IO (Option ParquetWriter) := do
  let opt_ptr ← parquet_writer_open_impl file_path schema.ptr
  match opt_ptr with
  | none => return none
  | some ptr =>
    return some { ptr := ptr, file_path := file_path, schema := schema }

def ParquetWriter.close (writer: ParquetWriter) : IO Unit :=
  parquet_writer_close_impl writer.ptr

def ParquetWriter.writeTable (writer: ParquetWriter) (stream: ArrowArrayStream) : IO Bool :=
  parquet_writer_write_table_impl writer.ptr stream.ptr

def ParquetWriter.writeBatch (writer: ParquetWriter) (array: ArrowArray) : IO Bool :=
  parquet_writer_write_batch_impl writer.ptr array.ptr

def ParquetWriter.setCompression (writer: ParquetWriter) (compression: ParquetCompression) : IO Unit :=
  parquet_writer_set_compression_impl writer.ptr (compressionToUInt32 compression)

-- High-level metadata operations
def ParquetFileMetadata.getRowGroup (metadata: ParquetFileMetadata) (index: UInt32) : IO (Option ParquetRowGroupMetadata) := do
  let opt_ptr ← parquet_metadata_get_row_group_impl metadata.ptr index
  match opt_ptr with
  | none => return none
  | some ptr => do
    let num_rows ← parquet_row_group_get_num_rows_impl ptr
    let num_columns ← parquet_row_group_get_num_columns_impl ptr
    let total_byte_size ← parquet_row_group_get_total_byte_size_impl ptr
    return some { ptr := ptr, num_rows := num_rows, num_columns := num_columns, total_byte_size := total_byte_size }

-- Convenience functions for common operations
def readParquetFile (file_path: String) : IO (Option ArrowArrayStream) := do
  let reader_opt ← ParquetReader.open file_path
  match reader_opt with
  | none => return none
  | some reader => do
    let result ← reader.readTable
    reader.close
    return result

def writeParquetFile (file_path: String) (schema: ArrowSchema) (stream: ArrowArrayStream) : IO Bool := do
  let writer_opt ← ParquetWriter.open file_path schema
  match writer_opt with
  | none => return false
  | some writer => do
    let success ← writer.writeTable stream
    writer.close
    return success
