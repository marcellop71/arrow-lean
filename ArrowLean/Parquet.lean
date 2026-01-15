-- Parquet support for Arrow-Lean
-- Provides bindings to libparquet for reading and writing Parquet files

import ArrowLean.Ops

-- Opaque pointer types for Parquet C structures
opaque ParquetReaderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque ParquetWriterPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque ParquetFileMetadataPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque ParquetRowGroupMetadataPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque ParquetColumnMetadataPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩

-- Provide explicit Nonempty instances for the opaque types
instance : Nonempty ParquetReaderPtr.type := ParquetReaderPtr.property
instance : Nonempty ParquetWriterPtr.type := ParquetWriterPtr.property
instance : Nonempty ParquetFileMetadataPtr.type := ParquetFileMetadataPtr.property
instance : Nonempty ParquetRowGroupMetadataPtr.type := ParquetRowGroupMetadataPtr.property
instance : Nonempty ParquetColumnMetadataPtr.type := ParquetColumnMetadataPtr.property

-- Parquet compression types
inductive ParquetCompression
| uncompressed
| snappy
| gzip
| lzo
| brotli
| lz4
| zstd
deriving Nonempty

-- Parquet encoding types
inductive ParquetEncoding
| plain
| dictionary
| rle
| bitPacked
| deltaByteArray
| deltaBinaryPacked
| deltaLengthByteArray
deriving Nonempty

-- Parquet file metadata wrapper
structure ParquetFileMetadata where
  ptr : ParquetFileMetadataPtr.type
  num_rows : UInt64
  num_row_groups : UInt32
  file_size : UInt64
  deriving Nonempty

-- Parquet row group metadata wrapper
structure ParquetRowGroupMetadata where
  ptr : ParquetRowGroupMetadataPtr.type
  num_rows : UInt64
  num_columns : UInt32
  total_byte_size : UInt64
  deriving Nonempty

-- Parquet column metadata wrapper
structure ParquetColumnMetadata where
  ptr : ParquetColumnMetadataPtr.type
  column_name : String
  logical_type : ArrowType
  compression : ParquetCompression
  encoding : ParquetEncoding
  deriving Nonempty

-- Parquet reader wrapper
structure ParquetReader where
  ptr : ParquetReaderPtr.type
  file_path : String
  metadata : Option ParquetFileMetadata
  deriving Nonempty

-- Parquet writer wrapper
structure ParquetWriter where
  ptr : ParquetWriterPtr.type
  file_path : String
  schema : ArrowSchema
  deriving Nonempty

-- Parquet write options
structure ParquetWriteOptions where
  compression : ParquetCompression := ParquetCompression.snappy
  enable_dictionary : Bool := true
  dictionary_pagesize_limit : UInt64 := 1024 * 1024 -- 1MB
  write_batch_size : UInt64 := 1024
  max_row_group_length : UInt64 := 64 * 1024 * 1024 -- 64MB
  enable_statistics : Bool := true

instance : Inhabited ParquetWriteOptions := ⟨⟨ParquetCompression.snappy, true, 1024 * 1024, 1024, 64 * 1024 * 1024, true⟩⟩

-- Parquet read options
structure ParquetReadOptions where
  use_threads : Bool := true
  memory_pool_size : UInt64 := 256 * 1024 * 1024 -- 256MB
  columns : Option (Array String) := none -- Read all columns if none
  row_groups : Option (Array UInt32) := none -- Read all row groups if none
  deriving Inhabited
