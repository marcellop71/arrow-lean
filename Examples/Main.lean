-- Main entry point for Arrow-Lean examples

import Examples.ParquetExample
import Examples.TypedBuildersExample
import ArrowLean
import ZlogLean

def testCsvToParquet : IO Unit := do
  IO.println "=== Testing CSV to Parquet conversion ==="

  -- Check if native Arrow is available
  let available ← Arrow.CSV.isNativeAvailable
  IO.println s!"Native Arrow available: {available}"

  -- Get Arrow version
  let version ← Arrow.CSV.getArrowVersion
  IO.println s!"Arrow version: {version}"

  -- Test CSV parsing (pure Lean - always works)
  let csvData := "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago"
  let parsed := Arrow.CSV.parseCSV csvData
  IO.println s!"Parsed CSV: {parsed.numRows} rows, {parsed.numCols} cols"
  IO.println s!"Headers: {parsed.headers}"

  -- Test native CSV to Parquet (only works if Arrow C++ is available)
  if available then
    IO.println "Testing native CSV to Parquet conversion..."
    let result ← Arrow.CSV.csvToParquet csvData "/tmp/test_arrow_lean.parquet" "snappy"
    match result with
    | .ok () => IO.println "Successfully wrote /tmp/test_arrow_lean.parquet"
    | .error e => IO.println s!"Error: {e}"
  else
    IO.println "Skipping native CSV to Parquet (Arrow C++ not available)"

def main : IO UInt32 := do
  Zlog.info "Running Arrow-Lean Examples"
  testCsvToParquet
  ParquetExample.main
  runTypedBuildersExamples
  return 0
