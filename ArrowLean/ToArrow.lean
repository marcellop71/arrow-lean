-- Typeclass-based serialization for converting Lean types to Arrow format

import ArrowLean.TypedBuilders
import ArrowLean.ParquetFFI
import ArrowLean.IPC

-- ============================================================================
-- ToArrowColumn: Convert array of values to Arrow column
-- ============================================================================

/-- Typeclass for types that can be serialized as Arrow columns -/
class ToArrowColumn (α : Type) where
  /-- Arrow format string for this type -/
  arrowFormat : String
  /-- Convert an array of values to an Arrow array -/
  toColumn : Array α → IO (Option ArrowArray)

instance : ToArrowColumn Int64 where
  arrowFormat := "l"  -- int64
  toColumn := buildInt64Column

instance : ToArrowColumn Float where
  arrowFormat := "g"  -- float64
  toColumn := buildFloat64Column

instance : ToArrowColumn String where
  arrowFormat := "u"  -- utf8 string
  toColumn := buildStringColumn

instance : ToArrowColumn Bool where
  arrowFormat := "b"  -- boolean
  toColumn := buildBoolColumn

-- Nat as Int64
instance : ToArrowColumn Nat where
  arrowFormat := "l"
  toColumn values := buildInt64Column (values.map (·.toInt64))

-- UInt64 (native support)
instance : ToArrowColumn UInt64 where
  arrowFormat := "L"  -- uint64
  toColumn := buildUInt64Column

-- Int8
instance : ToArrowColumn Int8 where
  arrowFormat := "c"  -- int8
  toColumn := buildInt8Column

-- Int16
instance : ToArrowColumn Int16 where
  arrowFormat := "s"  -- int16
  toColumn := buildInt16Column

-- Int32
instance : ToArrowColumn Int32 where
  arrowFormat := "i"  -- int32
  toColumn := buildInt32Column

-- UInt8
instance : ToArrowColumn UInt8 where
  arrowFormat := "C"  -- uint8
  toColumn := buildUInt8Column

-- UInt16
instance : ToArrowColumn UInt16 where
  arrowFormat := "S"  -- uint16
  toColumn := buildUInt16Column

-- UInt32
instance : ToArrowColumn UInt32 where
  arrowFormat := "I"  -- uint32
  toColumn := buildUInt32Column

-- ByteArray as Binary
instance : ToArrowColumn ByteArray where
  arrowFormat := "z"  -- binary
  toColumn := buildBinaryColumn

-- ============================================================================
-- ColumnSpec: Describe a column in a schema
-- ============================================================================

/-- Column specification for schema building -/
structure ColumnSpec where
  name : String
  format : String
  nullable : Bool := true

/-- Create a column spec for Int64 -/
def ColumnSpec.int64 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "l", nullable }

/-- Create a column spec for Float64 -/
def ColumnSpec.float64 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "g", nullable }

/-- Create a column spec for String -/
def ColumnSpec.string (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "u", nullable }

/-- Create a column spec for Bool -/
def ColumnSpec.bool (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "b", nullable }

/-- Create a column spec for Timestamp (microseconds) -/
def ColumnSpec.timestamp (name : String) (timezone : String := "UTC") (nullable : Bool := true) : ColumnSpec :=
  { name, format := s!"tsu:{timezone}", nullable }

/-- Create a column spec for Int8 -/
def ColumnSpec.int8 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "c", nullable }

/-- Create a column spec for Int16 -/
def ColumnSpec.int16 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "s", nullable }

/-- Create a column spec for Int32 -/
def ColumnSpec.int32 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "i", nullable }

/-- Create a column spec for UInt8 -/
def ColumnSpec.uint8 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "C", nullable }

/-- Create a column spec for UInt16 -/
def ColumnSpec.uint16 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "S", nullable }

/-- Create a column spec for UInt32 -/
def ColumnSpec.uint32 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "I", nullable }

/-- Create a column spec for UInt64 -/
def ColumnSpec.uint64 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "L", nullable }

/-- Create a column spec for Float32 -/
def ColumnSpec.float32 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "f", nullable }

/-- Create a column spec for Date32 (days since epoch) -/
def ColumnSpec.date32 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "tdD", nullable }

/-- Create a column spec for Date64 (milliseconds since epoch) -/
def ColumnSpec.date64 (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "tdm", nullable }

/-- Create a column spec for Time32 (seconds) -/
def ColumnSpec.time32s (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "tts", nullable }

/-- Create a column spec for Time32 (milliseconds) -/
def ColumnSpec.time32m (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "ttm", nullable }

/-- Create a column spec for Time64 (microseconds) -/
def ColumnSpec.time64u (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "ttu", nullable }

/-- Create a column spec for Time64 (nanoseconds) -/
def ColumnSpec.time64n (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "ttn", nullable }

/-- Create a column spec for Duration (microseconds) -/
def ColumnSpec.durationUs (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "tDu", nullable }

/-- Create a column spec for Binary -/
def ColumnSpec.binary (name : String) (nullable : Bool := true) : ColumnSpec :=
  { name, format := "z", nullable }

-- ============================================================================
-- ToArrowBatch: Convert array of records to Arrow batch
-- ============================================================================

/-- Typeclass for types that can be serialized as Arrow record batches -/
class ToArrowBatch (α : Type) where
  /-- Column specifications for the schema -/
  columnSpecs : Array ColumnSpec
  /-- Build Arrow columns from an array of records -/
  buildColumns : Array α → IO (Option (Array ArrowArray))

/-- Extract timezone from format string like "tsu:UTC" -/
private def extractTimezone (format : String) : String :=
  if format.length > 4 then
    (format.drop 4).toString
  else
    "UTC"

/-- Build a schema from column specs -/
def buildSchemaFromSpecs (specs : Array ColumnSpec) : IO (Option ArrowSchema) := do
  match ← SchemaBuilder.create specs.size.toUSize with
  | some sb =>
    for spec in specs do
      let ok ← match spec.format with
        -- Integer types
        | "c" => sb.addInt8 spec.name spec.nullable
        | "s" => sb.addInt16 spec.name spec.nullable
        | "i" => sb.addInt32 spec.name spec.nullable
        | "l" => sb.addInt64 spec.name spec.nullable
        | "C" => sb.addUInt8 spec.name spec.nullable
        | "S" => sb.addUInt16 spec.name spec.nullable
        | "I" => sb.addUInt32 spec.name spec.nullable
        | "L" => sb.addUInt64 spec.name spec.nullable
        -- Floating point
        | "f" => sb.addFloat32 spec.name spec.nullable
        | "g" => sb.addFloat64 spec.name spec.nullable
        -- String/Binary
        | "u" => sb.addString spec.name spec.nullable
        | "z" => sb.addBinary spec.name spec.nullable
        -- Boolean
        | "b" => sb.addBool spec.name spec.nullable
        -- Date types
        | "tdD" => sb.addDate32 spec.name spec.nullable
        | "tdm" => sb.addDate64 spec.name spec.nullable
        -- Time types
        | "tts" => sb.addTime32 spec.name Time32Unit.seconds spec.nullable
        | "ttm" => sb.addTime32 spec.name Time32Unit.milliseconds spec.nullable
        | "ttu" => sb.addTime64 spec.name Time64Unit.microseconds spec.nullable
        | "ttn" => sb.addTime64 spec.name Time64Unit.nanoseconds spec.nullable
        -- Duration types
        | "tDs" => sb.addDuration spec.name DurationUnit.seconds spec.nullable
        | "tDm" => sb.addDuration spec.name DurationUnit.milliseconds spec.nullable
        | "tDu" => sb.addDuration spec.name DurationUnit.microseconds spec.nullable
        | "tDn" => sb.addDuration spec.name DurationUnit.nanoseconds spec.nullable
        -- Timestamp (format: tsu:timezone)
        | fmt =>
          if fmt.startsWith "tsu:" then
            let tz := extractTimezone fmt
            sb.addTimestamp spec.name tz spec.nullable
          else
            pure false
      if !ok then return none
    sb.finish
  | none => return none

/-- Convert an array of records to a RecordBatch -/
def toRecordBatch [ToArrowBatch α] (records : Array α) : IO (Option RecordBatch) := do
  -- Build schema
  let specs := ToArrowBatch.columnSpecs (α := α)
  match ← buildSchemaFromSpecs specs with
  | some schema =>
    -- Build columns
    match ← ToArrowBatch.buildColumns records with
    | some columns =>
      if columns.size != specs.size then
        return none
      RecordBatch.create schema columns records.size.toUSize
    | none => return none
  | none => return none

/-- Convert records to an ArrowArrayStream -/
def toArrowStream [ToArrowBatch α] (records : Array α) : IO (Option ArrowArrayStream) := do
  match ← toRecordBatch records with
  | some batch => batch.toStream
  | none => return none

-- ============================================================================
-- Parquet Writing (uses existing ParquetFFI)
-- ============================================================================

/-- Write records directly to a Parquet file -/
def writeRecordsToParquet [ToArrowBatch α] (path : String) (records : Array α) : IO Bool := do
  -- Build schema
  let specs := ToArrowBatch.columnSpecs (α := α)
  match ← buildSchemaFromSpecs specs with
  | some schema =>
    -- Build stream
    match ← toArrowStream records with
    | some stream =>
      -- Use the ParquetFFI to write
      writeParquetFile path schema stream
    | none => return false
  | none => return false

-- ============================================================================
-- IPC Serialization (pure C, no external dependencies)
-- ============================================================================

/-- Serialize records to Arrow IPC format (pure C implementation) -/
def serializeRecordsToIPC [ToArrowBatch α] (records : Array α) : IO (Option ByteArray) := do
  match ← toRecordBatch records with
  | some batch =>
    -- Get the struct array
    match ← batch.toStructArray with
    | some arr =>
      -- Build schema for serialization
      let specs := ToArrowBatch.columnSpecs (α := α)
      match ← buildSchemaFromSpecs specs with
      | some schema =>
        -- Use IPC serialization
        let bytes ← ArrowLean.IPC.serializePair schema arr
        return some bytes
      | none => return none
    | none => return none
  | none => return none

/-- Serialize records to IPC and write to file -/
def writeRecordsToIPC [ToArrowBatch α] (path : String) (records : Array α) : IO Bool := do
  match ← serializeRecordsToIPC records with
  | some bytes =>
    IO.FS.writeBinFile path bytes
    return true
  | none => return false

-- ============================================================================
-- Helper: Projecting columns from records
-- ============================================================================

/-- Helper to extract a column from an array of records -/
def projectColumn (α β : Type) (records : Array α) (f : α → β) : Array β :=
  records.map f

/-- Helper to extract an optional column -/
def projectOptColumn (α β : Type) (records : Array α) (f : α → Option β) : Array (Option β) :=
  records.map f

-- ============================================================================
-- Convenience builders for optional columns
-- ============================================================================

/-- Build an Int64 column with optional values -/
def buildOptInt64Column (values : Array (Option Int64)) : IO (Option ArrowArray) := do
  match ← Int64Builder.create values.size.toUSize with
  | some builder =>
    for v in values do
      let _ ← builder.appendOption v
    builder.finish
  | none => return none

/-- Build a Float64 column with optional values -/
def buildOptFloat64Column (values : Array (Option Float)) : IO (Option ArrowArray) := do
  match ← Float64Builder.create values.size.toUSize with
  | some builder =>
    for v in values do
      let _ ← builder.appendOption v
    builder.finish
  | none => return none

/-- Build a String column with optional values -/
def buildOptStringColumn (values : Array (Option String)) : IO (Option ArrowArray) := do
  match ← StringBuilder.create values.size.toUSize with
  | some builder =>
    for v in values do
      let _ ← builder.appendOption v
    builder.finish
  | none => return none

/-- Build a Bool column with optional values -/
def buildOptBoolColumn (values : Array (Option Bool)) : IO (Option ArrowArray) := do
  match ← BoolBuilder.create values.size.toUSize with
  | some builder =>
    for v in values do
      let _ ← builder.appendOption v
    builder.finish
  | none => return none

/-- Build a Timestamp column with optional values -/
def buildOptTimestampColumn (values : Array (Option Int64)) (timezone : String := "UTC") : IO (Option ArrowArray) := do
  match ← TimestampBuilder.create values.size.toUSize timezone with
  | some builder =>
    for v in values do
      let _ ← builder.appendOption v
    builder.finish
  | none => return none

-- ============================================================================
-- Example Usage Pattern
-- ============================================================================

/-
To use this with a custom type like Trade:

structure Trade where
  timestamp : Int64
  symbol : Option String
  price : Float
  size : Nat

instance : ToArrowBatch Trade where
  columnSpecs := #[
    ColumnSpec.timestamp "timestamp" "UTC" false,
    ColumnSpec.string "symbol" true,
    ColumnSpec.float64 "price" false,
    ColumnSpec.int64 "size" false
  ]

  buildColumns trades := do
    -- Build timestamp column
    let tsCol ← buildTimestampColumn (trades.map (·.timestamp))
    -- Build symbol column (with nulls)
    let symCol ← buildOptStringColumn (trades.map (·.symbol))
    -- Build price column
    let priceCol ← buildFloat64Column (trades.map (·.price))
    -- Build size column
    let sizeCol ← buildInt64Column (trades.map (·.size.toInt64))

    match tsCol, symCol, priceCol, sizeCol with
    | some ts, some sym, some price, some size =>
      return some #[ts, sym, price, size]
    | _, _, _, _ => return none

-- Then use:
-- let trades : Array Trade := ...
-- let ok ← writeRecordsToParquet "trades.parquet" trades
-- or
-- let ok ← writeRecordsToIPC "trades.arrow" trades
-/
