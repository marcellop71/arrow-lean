-- Example: Using TypedBuilders and ToArrow for direct Arrow construction
-- This demonstrates the recommended approach for converting Lean data to Arrow format

import ArrowLean
import ZlogLean

open ArrowLean

-- ============================================================================
-- Example 1: Building individual columns with TypedBuilders
-- ============================================================================

def buildColumnsExample : IO Unit := do
  IO.println "=== Building Individual Columns ==="

  -- Build an Int64 column
  let int64Values : Array Int64 := #[100, 200, 300, 400, 500]
  match ← buildInt64Column int64Values with
  | some arr =>
    IO.println s!"Created Int64 column with {arr.length} values"
    arr.release
  | none =>
    IO.println "Failed to create Int64 column"

  -- Build a Float64 column
  let floatValues : Array Float := #[1.5, 2.7, 3.14, 4.0, 5.5]
  match ← buildFloat64Column floatValues with
  | some arr =>
    IO.println s!"Created Float64 column with {arr.length} values"
    arr.release
  | none =>
    IO.println "Failed to create Float64 column"

  -- Build a String column
  let stringValues : Array String := #["apple", "banana", "cherry", "date", "elderberry"]
  match ← buildStringColumn stringValues with
  | some arr =>
    IO.println s!"Created String column with {arr.length} values"
    arr.release
  | none =>
    IO.println "Failed to create String column"

  -- Build a Bool column
  let boolValues : Array Bool := #[true, false, true, true, false]
  match ← buildBoolColumn boolValues with
  | some arr =>
    IO.println s!"Created Bool column with {arr.length} values"
    arr.release
  | none =>
    IO.println "Failed to create Bool column"

  IO.println ""

-- ============================================================================
-- Example 2: Building columns with nullable values
-- ============================================================================

def buildNullableColumnsExample : IO Unit := do
  IO.println "=== Building Nullable Columns ==="

  -- Int64 with some null values
  let optInt64Values : Array (Option Int64) := #[some 10, none, some 30, none, some 50]
  match ← buildOptInt64Column optInt64Values with
  | some arr =>
    IO.println s!"Created nullable Int64 column with {arr.length} values, {arr.null_count} nulls"
    arr.release
  | none =>
    IO.println "Failed to create nullable Int64 column"

  -- String with some null values
  let optStringValues : Array (Option String) := #[some "hello", none, some "world", some "test", none]
  match ← buildOptStringColumn optStringValues with
  | some arr =>
    IO.println s!"Created nullable String column with {arr.length} values, {arr.null_count} nulls"
    arr.release
  | none =>
    IO.println "Failed to create nullable String column"

  IO.println ""

-- ============================================================================
-- Example 3: Building a complete RecordBatch
-- ============================================================================

def buildRecordBatchExample : IO Unit := do
  IO.println "=== Building a RecordBatch ==="

  -- Create schema with multiple columns
  match ← SchemaBuilder.create 3 with
  | some sb =>
    let _ ← sb.addInt64 "id" false           -- non-nullable
    let _ ← sb.addString "name" true         -- nullable
    let _ ← sb.addFloat64 "score" false      -- non-nullable

    match ← sb.finish with
    | some schema =>
      -- Build columns
      let ids ← buildInt64Column #[1, 2, 3, 4, 5]
      let names ← buildOptStringColumn #[some "Alice", some "Bob", none, some "Diana", some "Eve"]
      let scores ← buildFloat64Column #[95.5, 87.3, 92.1, 88.9, 91.0]

      match ids, names, scores with
      | some idArr, some nameArr, some scoreArr =>
        -- Create RecordBatch
        match ← RecordBatch.create schema #[idArr, nameArr, scoreArr] 5 with
        | some batch =>
          IO.println s!"Created RecordBatch with {batch.numRows} rows and {batch.numColumns} columns"

          -- Convert to stream for further processing
          match ← batch.toStream with
          | some stream =>
            IO.println "Successfully converted to ArrowArrayStream"
            stream.release
          | none =>
            IO.println "Failed to convert to stream"
        | none =>
          IO.println "Failed to create RecordBatch"
          idArr.release
          nameArr.release
          scoreArr.release
          schema.release
      | _, _, _ =>
        IO.println "Failed to build columns"
        schema.release
    | none =>
      IO.println "Failed to finish schema"
  | none =>
    IO.println "Failed to create SchemaBuilder"

  IO.println ""

-- ============================================================================
-- Example 4: Using ToArrowBatch typeclass for custom types
-- ============================================================================

-- Define a custom data type
structure Trade where
  timestamp : Int64      -- Microseconds since epoch
  symbol : Option String -- Stock symbol (nullable)
  price : Float          -- Trade price
  quantity : Nat         -- Number of shares

-- Implement ToArrowBatch for Trade
instance : ToArrowBatch Trade where
  columnSpecs := #[
    ColumnSpec.timestamp "timestamp" "UTC" false,
    ColumnSpec.string "symbol" true,
    ColumnSpec.float64 "price" false,
    ColumnSpec.int64 "quantity" false
  ]

  buildColumns trades := do
    -- Build timestamp column
    let tsCol ← buildTimestampColumn (trades.map (·.timestamp))
    -- Build symbol column (with nulls)
    let symCol ← buildOptStringColumn (trades.map (·.symbol))
    -- Build price column
    let priceCol ← buildFloat64Column (trades.map (·.price))
    -- Build quantity column
    let qtyCol ← buildInt64Column (trades.map (·.quantity.toInt64))

    match tsCol, symCol, priceCol, qtyCol with
    | some ts, some sym, some price, some qty =>
      return some #[ts, sym, price, qty]
    | _, _, _, _ => return none

def toArrowBatchExample : IO Unit := do
  IO.println "=== Using ToArrowBatch Typeclass ==="

  -- Sample trade data
  let trades : Array Trade := #[
    { timestamp := 1705593600000000, symbol := some "AAPL", price := 185.50, quantity := 100 },
    { timestamp := 1705593601000000, symbol := some "GOOGL", price := 141.25, quantity := 50 },
    { timestamp := 1705593602000000, symbol := none, price := 375.00, quantity := 25 },
    { timestamp := 1705593603000000, symbol := some "MSFT", price := 390.75, quantity := 75 },
    { timestamp := 1705593604000000, symbol := some "AMZN", price := 155.30, quantity := 200 }
  ]

  -- Convert to RecordBatch using the typeclass
  match ← toRecordBatch trades with
  | some batch =>
    IO.println s!"Created RecordBatch from {trades.size} Trade records"
    IO.println s!"  Rows: {batch.numRows}"
    IO.println s!"  Columns: {batch.numColumns}"

    -- Optionally write to IPC format
    match ← serializeRecordsToIPC trades with
    | some bytes =>
      IO.println s!"Serialized to IPC: {bytes.size} bytes"
    | none =>
      IO.println "Failed to serialize to IPC"

  | none =>
    IO.println "Failed to convert trades to RecordBatch"

  IO.println ""

-- ============================================================================
-- Example 5: Writing to Parquet (if available)
-- ============================================================================

def writeParquetExample : IO Unit := do
  IO.println "=== Writing to Parquet ==="

  -- Pure C Parquet writer is always available (no external dependencies)
  IO.println "Parquet support: pure C implementation (always available)"

  -- Sample data
  let trades : Array Trade := #[
    { timestamp := 1705593600000000, symbol := some "AAPL", price := 185.50, quantity := 100 },
    { timestamp := 1705593601000000, symbol := some "GOOGL", price := 141.25, quantity := 50 },
    { timestamp := 1705593602000000, symbol := some "TSLA", price := 248.50, quantity := 30 }
  ]

  -- Write using ToArrowBatch to Parquet
  let ok ← writeRecordsToParquet "/tmp/trades_example.parquet" trades
  if ok then
    IO.println "Successfully wrote /tmp/trades_example.parquet"
  else
    IO.println "Failed to write Parquet file"

  -- Also demonstrate IPC writing
  let ok ← writeRecordsToIPC "/tmp/trades_example.arrow" trades
  if ok then
    IO.println "Successfully wrote /tmp/trades_example.arrow (IPC format)"
  else
    IO.println "Failed to write IPC file"

  IO.println ""

-- ============================================================================
-- Example 6: Using low-level builders directly
-- ============================================================================

def lowLevelBuilderExample : IO Unit := do
  IO.println "=== Low-Level Builder API ==="

  -- Create an Int64 builder with explicit control
  match ← Int64Builder.create 10 with
  | some builder =>
    -- Append values one by one
    let _ ← builder.append 100
    let _ ← builder.append 200
    let _ ← builder.appendNull  -- Explicit null
    let _ ← builder.append 400

    -- Finish and get the array
    match ← builder.finish with
    | some arr =>
      IO.println s!"Created array with {arr.length} values, {arr.null_count} nulls"

      -- Access individual values
      for i in [:arr.length.toNat] do
        match ← arr.getInt64 i.toUSize with
        | some v => IO.println s!"  [{i}] = {v}"
        | none => IO.println s!"  [{i}] = null"

      arr.release
    | none =>
      IO.println "Failed to finish builder"
  | none =>
    IO.println "Failed to create builder"

  IO.println ""

-- ============================================================================
-- Main entry point
-- ============================================================================

def runTypedBuildersExamples : IO Unit := do
  IO.println "╔══════════════════════════════════════════════════════════════╗"
  IO.println "║           Arrow-Lean TypedBuilders Examples                   ║"
  IO.println "╚══════════════════════════════════════════════════════════════╝"
  IO.println ""

  buildColumnsExample
  buildNullableColumnsExample
  buildRecordBatchExample
  toArrowBatchExample
  writeParquetExample
  lowLevelBuilderExample

  IO.println "All examples completed!"
