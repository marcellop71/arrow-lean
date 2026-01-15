-- CSV parsing and conversion utilities
-- Provides in-memory CSV to Parquet conversion using native Arrow C++ libraries

import ZlogLean

namespace Arrow.CSV

open Zlog

/-- CSV parsing result -/
structure CSVData where
  headers : Array String
  rows : Array (Array String)
  numRows : Nat
  numCols : Nat
  deriving Repr

/-- Parse a CSV string into structured data -/
def parseCSV (csv : String) (delimiter : Char := ',') : CSVData := Id.run do
  let lines := csv.splitOn "\n" |>.filter (·.length > 0)
  if lines.isEmpty then
    return { headers := #[], rows := #[], numRows := 0, numCols := 0 }

  -- Parse header
  let headerLine := lines.head!
  let headers := parseRow headerLine delimiter

  -- Parse data rows
  let dataLines := lines.tail!
  let mut rows : Array (Array String) := #[]
  for line in dataLines do
    let row := parseRow line delimiter
    if row.size > 0 then
      rows := rows.push row

  { headers := headers
    rows := rows
    numRows := rows.size
    numCols := headers.size }
where
  parseRow (line : String) (delim : Char) : Array String :=
    line.splitOn (String.singleton delim) |>.map (·.trimAscii.toString) |>.toArray

-- Native FFI declarations for CSV to Parquet conversion
@[extern "lean_csv_to_parquet"]
private opaque csvToParquetNative (csv : @& String) (outputPath : @& String) (compression : @& String) : IO (Except String Unit)

@[extern "lean_merge_csv_to_parquet"]
private opaque mergeCsvToParquetNative (csvStrings : @& Array String) (outputPath : @& String) (compression : @& String) : IO (Except String Unit)

@[extern "lean_arrow_parquet_available"]
private opaque arrowParquetAvailableNative : IO Bool

@[extern "lean_arrow_version"]
private opaque arrowVersionNative : IO String

/-- Check if native Arrow/Parquet support is available -/
def isNativeAvailable : IO Bool := arrowParquetAvailableNative

/-- Get Arrow library version -/
def getArrowVersion : IO String := arrowVersionNative

/-- Convert CSV string to Parquet file using native Arrow C++ library
    This function uses libarrow for CSV parsing and libparquet for writing -/
def csvToParquet (csv : String) (outputPath : String) (compression : String := "snappy") : IO (Except String Unit) := do
  if csv.isEmpty then
    return .error "Empty CSV data"
  csvToParquetNative csv outputPath compression

/-- Convert in-memory CSV data directly to Parquet (convenience wrapper) -/
def csvDataToParquet (data : CSVData) (outputPath : String) (compression : String := "snappy") : IO (Except String Unit) := do
  -- Reconstruct CSV string
  let headerLine := String.intercalate "," data.headers.toList
  let dataLines := data.rows.map (fun row => String.intercalate "," row.toList)
  let csv := headerLine ++ "\n" ++ String.intercalate "\n" dataLines.toList
  csvToParquet csv outputPath compression

/-- Merge multiple CSV strings into a single Parquet file using native Arrow C++ library -/
def mergeCsvToParquet (csvStrings : Array String) (outputPath : String) (compression : String := "snappy") : IO (Except String Unit) := do
  if csvStrings.isEmpty then
    return .ok ()
  -- Filter out empty strings
  let nonEmpty := csvStrings.filter (·.length > 0)
  if nonEmpty.isEmpty then
    return .ok ()
  mergeCsvToParquetNative nonEmpty outputPath compression

end Arrow.CSV
