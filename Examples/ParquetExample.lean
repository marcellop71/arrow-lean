-- Example usage of Parquet bindings in Arrow-Lean
-- Demonstrates reading and writing Parquet files

import ArrowLean
import ZlogLean

namespace ParquetExample

-- Example: Read a Parquet file
def readParquetExample (file_path: String) : IO Unit := do
  Zlog.info s!"Reading Parquet file: {file_path}"

  -- Open the Parquet file
  let reader_opt ← ParquetReader.open file_path
  match reader_opt with
  | none =>
    Zlog.error "Failed to open Parquet file"
  | some reader => do
    Zlog.info "Successfully opened Parquet file"

    -- Get file metadata
    let metadata_opt ← reader.getMetadata
    match metadata_opt with
    | none =>
      Zlog.error "Failed to get metadata"
    | some metadata => do
      Zlog.debug s!"Number of rows: {metadata.num_rows}"
      Zlog.debug s!"Number of row groups: {metadata.num_row_groups}"
      Zlog.debug s!"File size: {metadata.file_size} bytes"

      -- Read the entire table
      let stream_opt ← reader.readTable
      match stream_opt with
      | none =>
        Zlog.error "Failed to read table"
      | some stream => do
        Zlog.info "Successfully read table as Arrow stream"

        -- Get schema from stream
        let schema_opt ← stream.getSchema
        match schema_opt with
        | none =>
          Zlog.warn "No schema found"
        | some schema => do
          Zlog.debug s!"Schema format: {schema.format}"
          Zlog.debug s!"Schema name: {schema.name}"

    -- Close the reader
    reader.close
    Zlog.info "Closed Parquet reader"

-- Example: Write Arrow data to Parquet file
def writeParquetExample (file_path: String) : IO Unit := do
  Zlog.info s!"Writing Parquet file: {file_path}"

  -- Create a simple schema for demonstration
  let schema ← ArrowSchema.init "struct"

  -- Open Parquet writer
  let writer_opt ← ParquetWriter.open file_path schema
  match writer_opt with
  | none =>
    Zlog.error "Failed to open Parquet writer"
  | some writer => do
    Zlog.info "Successfully opened Parquet writer"

    -- Set compression
    writer.setCompression ParquetCompression.snappy
    Zlog.debug "Set compression to Snappy"

    -- Create some sample data
    let array ← ArrowArray.init 100

    -- Write the data
    let success ← writer.writeBatch array
    if success then
      Zlog.info "Successfully wrote data batch"
    else
      Zlog.error "Failed to write data batch"

    -- Close the writer
    writer.close
    Zlog.info "Closed Parquet writer"

    -- Release resources
    array.release
    schema.release

-- Example: Read specific columns from Parquet file
def readColumnsExample (file_path: String) (columns: Array String) : IO Unit := do
  Zlog.info s!"Reading columns {columns} from: {file_path}"

  let reader_opt ← ParquetReader.open file_path
  match reader_opt with
  | none =>
    Zlog.error "Failed to open Parquet file"
  | some reader => do
    -- Read only specific columns
    let stream_opt ← reader.readColumns columns
    match stream_opt with
    | none =>
      Zlog.error "Failed to read columns"
    | some stream => do
      Zlog.info "Successfully read specified columns"

      -- Process the stream data
      let next_opt ← stream.getNext
      match next_opt with
      | none =>
        Zlog.warn "No data in stream"
      | some array => do
        Zlog.debug s!"Got array with {array.length} rows"

    reader.close

-- Example: Read specific row group from Parquet file
def readRowGroupExample (file_path: String) (row_group_index: UInt32) : IO Unit := do
  Zlog.info s!"Reading row group {row_group_index} from: {file_path}"

  let reader_opt ← ParquetReader.open file_path
  match reader_opt with
  | none =>
    Zlog.error "Failed to open Parquet file"
  | some reader => do
    -- Read specific row group
    let stream_opt ← reader.readRowGroup row_group_index
    match stream_opt with
    | none =>
      Zlog.error "Failed to read row group"
    | some stream => do
      Zlog.info "Successfully read row group"

      -- Process the stream data
      let next_opt ← stream.getNext
      match next_opt with
      | none =>
        Zlog.warn "No data in row group"
      | some array => do
        Zlog.debug s!"Got array with {array.length} rows"

    reader.close

-- Convenience function for simple file copy with format conversion
def convertArrowToParquet (arrow_file: String) (parquet_file: String) : IO Bool := do
  Zlog.info s!"Converting {arrow_file} to {parquet_file}"

  -- This is a simplified example - in practice you'd need to implement
  -- Arrow file reading first, or use existing Arrow data
  let schema ← ArrowSchema.init "struct"
  let stream ← ArrowArrayStream.init

  let success ← writeParquetFile parquet_file schema stream

  schema.release
  return success

-- Main example function
def main : IO Unit := do
  Zlog.info "Arrow-Lean Parquet Examples"
  Zlog.info "==========================="

  -- Example file paths (uncomment examples below to use)
  let _input_file := "example_data.parquet"
  let _output_file := "output_data.parquet"

  -- Run examples (commented out since files may not exist)
  -- readParquetExample _input_file
  -- writeParquetExample _output_file
  -- readColumnsExample _input_file #["column1", "column2"]
  -- readRowGroupExample _input_file 0

  Zlog.info "Examples completed!"

end ParquetExample
