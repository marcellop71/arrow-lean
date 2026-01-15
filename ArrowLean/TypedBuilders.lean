-- High-level typed builder API for Arrow arrays

import ArrowLean.BuilderFFI
import ArrowLean.FFI

-- ============================================================================
-- Int64 Builder
-- ============================================================================

structure Int64Builder where
  ptr : Int64BuilderPtr.type

namespace Int64Builder

def create (capacity : USize := 1024) : IO (Option Int64Builder) := do
  match ← int64_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : Int64Builder) (value : Int64) : IO Bool :=
  int64_builder_append_impl builder.ptr value

def appendNull (builder : Int64Builder) : IO Bool :=
  int64_builder_append_null_impl builder.ptr

def appendOption (builder : Int64Builder) (value : Option Int64) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : Int64Builder) (values : Array Int64) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : Int64Builder) : IO USize :=
  int64_builder_length_impl builder.ptr

def finish (builder : Int64Builder) : IO (Option ArrowArray) := do
  match ← int64_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Int64Builder) : IO Unit :=
  int64_builder_free_impl builder.ptr

end Int64Builder

-- ============================================================================
-- Float64 Builder
-- ============================================================================

structure Float64Builder where
  ptr : Float64BuilderPtr.type

namespace Float64Builder

def create (capacity : USize := 1024) : IO (Option Float64Builder) := do
  match ← float64_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : Float64Builder) (value : Float) : IO Bool :=
  float64_builder_append_impl builder.ptr value

def appendNull (builder : Float64Builder) : IO Bool :=
  float64_builder_append_null_impl builder.ptr

def appendOption (builder : Float64Builder) (value : Option Float) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : Float64Builder) (values : Array Float) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : Float64Builder) : IO USize :=
  float64_builder_length_impl builder.ptr

def finish (builder : Float64Builder) : IO (Option ArrowArray) := do
  match ← float64_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Float64Builder) : IO Unit :=
  float64_builder_free_impl builder.ptr

end Float64Builder

-- ============================================================================
-- String Builder
-- ============================================================================

structure StringBuilder where
  ptr : StringBuilderPtr.type

namespace StringBuilder

def create (capacity : USize := 1024) (dataCapacity : USize := 8192) : IO (Option StringBuilder) := do
  match ← string_builder_create_impl capacity dataCapacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : StringBuilder) (value : String) : IO Bool :=
  string_builder_append_impl builder.ptr value

def appendNull (builder : StringBuilder) : IO Bool :=
  string_builder_append_null_impl builder.ptr

def appendOption (builder : StringBuilder) (value : Option String) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : StringBuilder) (values : Array String) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : StringBuilder) : IO USize :=
  string_builder_length_impl builder.ptr

def finish (builder : StringBuilder) : IO (Option ArrowArray) := do
  match ← string_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : StringBuilder) : IO Unit :=
  string_builder_free_impl builder.ptr

end StringBuilder

-- ============================================================================
-- Timestamp Builder
-- ============================================================================

structure TimestampBuilder where
  ptr : TimestampBuilderPtr.type
  timezone : String

namespace TimestampBuilder

def create (capacity : USize := 1024) (timezone : String := "UTC") : IO (Option TimestampBuilder) := do
  match ← timestamp_builder_create_impl capacity timezone with
  | some ptr => return some ⟨ptr, timezone⟩
  | none => return none

def append (builder : TimestampBuilder) (microseconds : Int64) : IO Bool :=
  timestamp_builder_append_impl builder.ptr microseconds

def appendNull (builder : TimestampBuilder) : IO Bool :=
  timestamp_builder_append_null_impl builder.ptr

def appendOption (builder : TimestampBuilder) (value : Option Int64) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : TimestampBuilder) (values : Array Int64) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : TimestampBuilder) : IO USize :=
  timestamp_builder_length_impl builder.ptr

def getTimezone (builder : TimestampBuilder) : IO String :=
  timestamp_builder_get_timezone_impl builder.ptr

def finish (builder : TimestampBuilder) : IO (Option ArrowArray) := do
  match ← timestamp_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : TimestampBuilder) : IO Unit :=
  timestamp_builder_free_impl builder.ptr

end TimestampBuilder

-- ============================================================================
-- Bool Builder
-- ============================================================================

structure BoolBuilder where
  ptr : BoolBuilderPtr.type

namespace BoolBuilder

def create (capacity : USize := 1024) : IO (Option BoolBuilder) := do
  match ← bool_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : BoolBuilder) (value : Bool) : IO Bool :=
  bool_builder_append_impl builder.ptr (if value then 1 else 0)

def appendNull (builder : BoolBuilder) : IO Bool :=
  bool_builder_append_null_impl builder.ptr

def appendOption (builder : BoolBuilder) (value : Option Bool) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : BoolBuilder) (values : Array Bool) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : BoolBuilder) : IO USize :=
  bool_builder_length_impl builder.ptr

def finish (builder : BoolBuilder) : IO (Option ArrowArray) := do
  match ← bool_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : BoolBuilder) : IO Unit :=
  bool_builder_free_impl builder.ptr

end BoolBuilder

-- ============================================================================
-- Int8 Builder
-- ============================================================================

structure Int8Builder where
  ptr : Int8BuilderPtr.type

namespace Int8Builder

def create (capacity : USize := 1024) : IO (Option Int8Builder) := do
  match ← int8_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : Int8Builder) (value : Int8) : IO Bool :=
  int8_builder_append_impl builder.ptr value

def appendNull (builder : Int8Builder) : IO Bool :=
  int8_builder_append_null_impl builder.ptr

def appendOption (builder : Int8Builder) (value : Option Int8) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : Int8Builder) (values : Array Int8) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : Int8Builder) : IO USize :=
  int8_builder_length_impl builder.ptr

def finish (builder : Int8Builder) : IO (Option ArrowArray) := do
  match ← int8_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Int8Builder) : IO Unit :=
  int8_builder_free_impl builder.ptr

end Int8Builder

-- ============================================================================
-- Int16 Builder
-- ============================================================================

structure Int16Builder where
  ptr : Int16BuilderPtr.type

namespace Int16Builder

def create (capacity : USize := 1024) : IO (Option Int16Builder) := do
  match ← int16_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : Int16Builder) (value : Int16) : IO Bool :=
  int16_builder_append_impl builder.ptr value

def appendNull (builder : Int16Builder) : IO Bool :=
  int16_builder_append_null_impl builder.ptr

def appendOption (builder : Int16Builder) (value : Option Int16) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : Int16Builder) (values : Array Int16) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : Int16Builder) : IO USize :=
  int16_builder_length_impl builder.ptr

def finish (builder : Int16Builder) : IO (Option ArrowArray) := do
  match ← int16_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Int16Builder) : IO Unit :=
  int16_builder_free_impl builder.ptr

end Int16Builder

-- ============================================================================
-- Int32 Builder
-- ============================================================================

structure Int32Builder where
  ptr : Int32BuilderPtr.type

namespace Int32Builder

def create (capacity : USize := 1024) : IO (Option Int32Builder) := do
  match ← int32_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : Int32Builder) (value : Int32) : IO Bool :=
  int32_builder_append_impl builder.ptr value

def appendNull (builder : Int32Builder) : IO Bool :=
  int32_builder_append_null_impl builder.ptr

def appendOption (builder : Int32Builder) (value : Option Int32) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : Int32Builder) (values : Array Int32) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : Int32Builder) : IO USize :=
  int32_builder_length_impl builder.ptr

def finish (builder : Int32Builder) : IO (Option ArrowArray) := do
  match ← int32_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Int32Builder) : IO Unit :=
  int32_builder_free_impl builder.ptr

end Int32Builder

-- ============================================================================
-- UInt8 Builder
-- ============================================================================

structure UInt8Builder where
  ptr : UInt8BuilderPtr.type

namespace UInt8Builder

def create (capacity : USize := 1024) : IO (Option UInt8Builder) := do
  match ← uint8_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : UInt8Builder) (value : UInt8) : IO Bool :=
  uint8_builder_append_impl builder.ptr value

def appendNull (builder : UInt8Builder) : IO Bool :=
  uint8_builder_append_null_impl builder.ptr

def appendOption (builder : UInt8Builder) (value : Option UInt8) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : UInt8Builder) (values : Array UInt8) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : UInt8Builder) : IO USize :=
  uint8_builder_length_impl builder.ptr

def finish (builder : UInt8Builder) : IO (Option ArrowArray) := do
  match ← uint8_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : UInt8Builder) : IO Unit :=
  uint8_builder_free_impl builder.ptr

end UInt8Builder

-- ============================================================================
-- UInt16 Builder
-- ============================================================================

structure UInt16Builder where
  ptr : UInt16BuilderPtr.type

namespace UInt16Builder

def create (capacity : USize := 1024) : IO (Option UInt16Builder) := do
  match ← uint16_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : UInt16Builder) (value : UInt16) : IO Bool :=
  uint16_builder_append_impl builder.ptr value

def appendNull (builder : UInt16Builder) : IO Bool :=
  uint16_builder_append_null_impl builder.ptr

def appendOption (builder : UInt16Builder) (value : Option UInt16) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : UInt16Builder) (values : Array UInt16) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : UInt16Builder) : IO USize :=
  uint16_builder_length_impl builder.ptr

def finish (builder : UInt16Builder) : IO (Option ArrowArray) := do
  match ← uint16_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : UInt16Builder) : IO Unit :=
  uint16_builder_free_impl builder.ptr

end UInt16Builder

-- ============================================================================
-- UInt32 Builder
-- ============================================================================

structure UInt32Builder where
  ptr : UInt32BuilderPtr.type

namespace UInt32Builder

def create (capacity : USize := 1024) : IO (Option UInt32Builder) := do
  match ← uint32_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : UInt32Builder) (value : UInt32) : IO Bool :=
  uint32_builder_append_impl builder.ptr value

def appendNull (builder : UInt32Builder) : IO Bool :=
  uint32_builder_append_null_impl builder.ptr

def appendOption (builder : UInt32Builder) (value : Option UInt32) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : UInt32Builder) (values : Array UInt32) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : UInt32Builder) : IO USize :=
  uint32_builder_length_impl builder.ptr

def finish (builder : UInt32Builder) : IO (Option ArrowArray) := do
  match ← uint32_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : UInt32Builder) : IO Unit :=
  uint32_builder_free_impl builder.ptr

end UInt32Builder

-- ============================================================================
-- UInt64 Builder
-- ============================================================================

structure UInt64Builder where
  ptr : UInt64BuilderPtr.type

namespace UInt64Builder

def create (capacity : USize := 1024) : IO (Option UInt64Builder) := do
  match ← uint64_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : UInt64Builder) (value : UInt64) : IO Bool :=
  uint64_builder_append_impl builder.ptr value

def appendNull (builder : UInt64Builder) : IO Bool :=
  uint64_builder_append_null_impl builder.ptr

def appendOption (builder : UInt64Builder) (value : Option UInt64) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : UInt64Builder) (values : Array UInt64) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : UInt64Builder) : IO USize :=
  uint64_builder_length_impl builder.ptr

def finish (builder : UInt64Builder) : IO (Option ArrowArray) := do
  match ← uint64_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : UInt64Builder) : IO Unit :=
  uint64_builder_free_impl builder.ptr

end UInt64Builder

-- ============================================================================
-- Float32 Builder
-- ============================================================================

structure Float32Builder where
  ptr : Float32BuilderPtr.type

namespace Float32Builder

def create (capacity : USize := 1024) : IO (Option Float32Builder) := do
  match ← float32_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : Float32Builder) (value : Float) : IO Bool :=
  float32_builder_append_impl builder.ptr value

def appendNull (builder : Float32Builder) : IO Bool :=
  float32_builder_append_null_impl builder.ptr

def appendOption (builder : Float32Builder) (value : Option Float) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : Float32Builder) (values : Array Float) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : Float32Builder) : IO USize :=
  float32_builder_length_impl builder.ptr

def finish (builder : Float32Builder) : IO (Option ArrowArray) := do
  match ← float32_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Float32Builder) : IO Unit :=
  float32_builder_free_impl builder.ptr

end Float32Builder

-- ============================================================================
-- Date32 Builder (days since epoch)
-- ============================================================================

structure Date32Builder where
  ptr : Date32BuilderPtr.type

namespace Date32Builder

def create (capacity : USize := 1024) : IO (Option Date32Builder) := do
  match ← date32_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : Date32Builder) (days : Int32) : IO Bool :=
  date32_builder_append_impl builder.ptr days

def appendNull (builder : Date32Builder) : IO Bool :=
  date32_builder_append_null_impl builder.ptr

def appendOption (builder : Date32Builder) (value : Option Int32) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : Date32Builder) (values : Array Int32) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : Date32Builder) : IO USize :=
  date32_builder_length_impl builder.ptr

def finish (builder : Date32Builder) : IO (Option ArrowArray) := do
  match ← date32_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Date32Builder) : IO Unit :=
  date32_builder_free_impl builder.ptr

end Date32Builder

-- ============================================================================
-- Date64 Builder (milliseconds since epoch)
-- ============================================================================

structure Date64Builder where
  ptr : Date64BuilderPtr.type

namespace Date64Builder

def create (capacity : USize := 1024) : IO (Option Date64Builder) := do
  match ← date64_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : Date64Builder) (milliseconds : Int64) : IO Bool :=
  date64_builder_append_impl builder.ptr milliseconds

def appendNull (builder : Date64Builder) : IO Bool :=
  date64_builder_append_null_impl builder.ptr

def appendOption (builder : Date64Builder) (value : Option Int64) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : Date64Builder) (values : Array Int64) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : Date64Builder) : IO USize :=
  date64_builder_length_impl builder.ptr

def finish (builder : Date64Builder) : IO (Option ArrowArray) := do
  match ← date64_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Date64Builder) : IO Unit :=
  date64_builder_free_impl builder.ptr

end Date64Builder

-- ============================================================================
-- Time32 Builder (seconds or milliseconds)
-- ============================================================================

inductive Time32Unit where
  | seconds
  | milliseconds
deriving Repr

def Time32Unit.toCode : Time32Unit → UInt8
  | .seconds => 's'.toNat.toUInt8
  | .milliseconds => 'm'.toNat.toUInt8

structure Time32Builder where
  ptr : Time32BuilderPtr.type
  unit : Time32Unit

namespace Time32Builder

def create (capacity : USize := 1024) (unit : Time32Unit := .seconds) : IO (Option Time32Builder) := do
  match ← time32_builder_create_impl capacity unit.toCode with
  | some ptr => return some ⟨ptr, unit⟩
  | none => return none

def append (builder : Time32Builder) (value : Int32) : IO Bool :=
  time32_builder_append_impl builder.ptr value

def appendNull (builder : Time32Builder) : IO Bool :=
  time32_builder_append_null_impl builder.ptr

def appendOption (builder : Time32Builder) (value : Option Int32) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : Time32Builder) (values : Array Int32) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : Time32Builder) : IO USize :=
  time32_builder_length_impl builder.ptr

def finish (builder : Time32Builder) : IO (Option ArrowArray) := do
  match ← time32_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Time32Builder) : IO Unit :=
  time32_builder_free_impl builder.ptr

end Time32Builder

-- ============================================================================
-- Time64 Builder (microseconds or nanoseconds)
-- ============================================================================

inductive Time64Unit where
  | microseconds
  | nanoseconds
deriving Repr

def Time64Unit.toCode : Time64Unit → UInt8
  | .microseconds => 'u'.toNat.toUInt8
  | .nanoseconds => 'n'.toNat.toUInt8

structure Time64Builder where
  ptr : Time64BuilderPtr.type
  unit : Time64Unit

namespace Time64Builder

def create (capacity : USize := 1024) (unit : Time64Unit := .microseconds) : IO (Option Time64Builder) := do
  match ← time64_builder_create_impl capacity unit.toCode with
  | some ptr => return some ⟨ptr, unit⟩
  | none => return none

def append (builder : Time64Builder) (value : Int64) : IO Bool :=
  time64_builder_append_impl builder.ptr value

def appendNull (builder : Time64Builder) : IO Bool :=
  time64_builder_append_null_impl builder.ptr

def appendOption (builder : Time64Builder) (value : Option Int64) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : Time64Builder) (values : Array Int64) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : Time64Builder) : IO USize :=
  time64_builder_length_impl builder.ptr

def finish (builder : Time64Builder) : IO (Option ArrowArray) := do
  match ← time64_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Time64Builder) : IO Unit :=
  time64_builder_free_impl builder.ptr

end Time64Builder

-- ============================================================================
-- Duration Builder
-- ============================================================================

inductive DurationUnit where
  | seconds
  | milliseconds
  | microseconds
  | nanoseconds
deriving Repr

def DurationUnit.toCode : DurationUnit → UInt8
  | .seconds => 's'.toNat.toUInt8
  | .milliseconds => 'm'.toNat.toUInt8
  | .microseconds => 'u'.toNat.toUInt8
  | .nanoseconds => 'n'.toNat.toUInt8

structure DurationBuilder where
  ptr : DurationBuilderPtr.type
  unit : DurationUnit

namespace DurationBuilder

def create (capacity : USize := 1024) (unit : DurationUnit := .microseconds) : IO (Option DurationBuilder) := do
  match ← duration_builder_create_impl capacity unit.toCode with
  | some ptr => return some ⟨ptr, unit⟩
  | none => return none

def append (builder : DurationBuilder) (value : Int64) : IO Bool :=
  duration_builder_append_impl builder.ptr value

def appendNull (builder : DurationBuilder) : IO Bool :=
  duration_builder_append_null_impl builder.ptr

def appendOption (builder : DurationBuilder) (value : Option Int64) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : DurationBuilder) (values : Array Int64) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : DurationBuilder) : IO USize :=
  duration_builder_length_impl builder.ptr

def finish (builder : DurationBuilder) : IO (Option ArrowArray) := do
  match ← duration_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : DurationBuilder) : IO Unit :=
  duration_builder_free_impl builder.ptr

end DurationBuilder

-- ============================================================================
-- Binary Builder
-- ============================================================================

structure BinaryBuilder where
  ptr : BinaryBuilderPtr.type

namespace BinaryBuilder

def create (capacity : USize := 1024) (dataCapacity : USize := 8192) : IO (Option BinaryBuilder) := do
  match ← binary_builder_create_impl capacity dataCapacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : BinaryBuilder) (value : ByteArray) : IO Bool :=
  binary_builder_append_impl builder.ptr value

def appendNull (builder : BinaryBuilder) : IO Bool :=
  binary_builder_append_null_impl builder.ptr

def appendOption (builder : BinaryBuilder) (value : Option ByteArray) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : BinaryBuilder) (values : Array ByteArray) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : BinaryBuilder) : IO USize :=
  binary_builder_length_impl builder.ptr

def finish (builder : BinaryBuilder) : IO (Option ArrowArray) := do
  match ← binary_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : BinaryBuilder) : IO Unit :=
  binary_builder_free_impl builder.ptr

end BinaryBuilder

-- ============================================================================
-- Schema Builder
-- ============================================================================

structure SchemaBuilder where
  ptr : SchemaBuilderPtr.type

namespace SchemaBuilder

def create (capacity : USize := 16) : IO (Option SchemaBuilder) := do
  match ← schema_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def addInt64 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_int64_impl builder.ptr name (if nullable then 1 else 0)

def addFloat64 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_float64_impl builder.ptr name (if nullable then 1 else 0)

def addString (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_string_impl builder.ptr name (if nullable then 1 else 0)

def addTimestamp (builder : SchemaBuilder) (name : String) (timezone : String := "UTC") (nullable : Bool := true) : IO Bool :=
  schema_builder_add_timestamp_impl builder.ptr name timezone (if nullable then 1 else 0)

def addBool (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_bool_impl builder.ptr name (if nullable then 1 else 0)

def addInt8 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_int8_impl builder.ptr name (if nullable then 1 else 0)

def addInt16 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_int16_impl builder.ptr name (if nullable then 1 else 0)

def addInt32 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_int32_impl builder.ptr name (if nullable then 1 else 0)

def addUInt8 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_uint8_impl builder.ptr name (if nullable then 1 else 0)

def addUInt16 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_uint16_impl builder.ptr name (if nullable then 1 else 0)

def addUInt32 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_uint32_impl builder.ptr name (if nullable then 1 else 0)

def addUInt64 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_uint64_impl builder.ptr name (if nullable then 1 else 0)

def addFloat32 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_float32_impl builder.ptr name (if nullable then 1 else 0)

def addDate32 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_date32_impl builder.ptr name (if nullable then 1 else 0)

def addDate64 (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_date64_impl builder.ptr name (if nullable then 1 else 0)

def addTime32 (builder : SchemaBuilder) (name : String) (unit : Time32Unit := .seconds) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_time32_impl builder.ptr name unit.toCode (if nullable then 1 else 0)

def addTime64 (builder : SchemaBuilder) (name : String) (unit : Time64Unit := .microseconds) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_time64_impl builder.ptr name unit.toCode (if nullable then 1 else 0)

def addDuration (builder : SchemaBuilder) (name : String) (unit : DurationUnit := .microseconds) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_duration_impl builder.ptr name unit.toCode (if nullable then 1 else 0)

def addBinary (builder : SchemaBuilder) (name : String) (nullable : Bool := true) : IO Bool :=
  schema_builder_add_binary_impl builder.ptr name (if nullable then 1 else 0)

def fieldCount (builder : SchemaBuilder) : IO USize :=
  schema_builder_field_count_impl builder.ptr

def finish (builder : SchemaBuilder) : IO (Option ArrowSchema) := do
  match ← schema_builder_finish_impl builder.ptr with
  | some ptr =>
    let format ← arrow_schema_get_format_impl ptr
    let name ← arrow_schema_get_name_impl ptr
    let flags ← arrow_schema_get_flags_impl ptr
    return some { ptr, format, name, flags }
  | none => return none

def free (builder : SchemaBuilder) : IO Unit :=
  schema_builder_free_impl builder.ptr

end SchemaBuilder

-- ============================================================================
-- RecordBatch
-- ============================================================================

structure RecordBatch where
  ptr : RecordBatchPtr.type
  schema : ArrowSchema
  numRows : USize
  numColumns : USize

namespace RecordBatch

def create (schema : ArrowSchema) (columns : Array ArrowArray) (numRows : USize) : IO (Option RecordBatch) := do
  let columnPtrs := columns.map (·.ptr)
  match ← record_batch_create_impl schema.ptr columnPtrs numRows with
  | some ptr => return some ⟨ptr, schema, numRows, columns.size.toUSize⟩
  | none => return none

def getNumRows (batch : RecordBatch) : IO USize :=
  record_batch_num_rows_impl batch.ptr

def getNumColumns (batch : RecordBatch) : IO USize :=
  record_batch_num_columns_impl batch.ptr

def toStructArray (batch : RecordBatch) : IO (Option ArrowArray) := do
  match ← record_batch_to_struct_array_impl batch.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def toStream (batch : RecordBatch) : IO (Option ArrowArrayStream) := do
  match ← batch_to_stream_impl batch.ptr with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def free (batch : RecordBatch) : IO Unit :=
  record_batch_free_impl batch.ptr

end RecordBatch

-- ============================================================================
-- Multiple Batches to Stream
-- ============================================================================

def batchesToStream (schema : ArrowSchema) (batches : Array RecordBatch) : IO (Option ArrowArrayStream) := do
  let batchPtrs := batches.map (·.ptr)
  match ← batches_to_stream_impl schema.ptr batchPtrs with
  | some ptr => return some ⟨ptr⟩
  | none => return none

-- ============================================================================
-- Convenience: Build columns from arrays
-- ============================================================================

def buildInt64Column (values : Array Int64) : IO (Option ArrowArray) := do
  match ← Int64Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildFloat64Column (values : Array Float) : IO (Option ArrowArray) := do
  match ← Float64Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildStringColumn (values : Array String) : IO (Option ArrowArray) := do
  -- Estimate data capacity
  let dataSize := values.foldl (fun acc s => acc + s.length) 0
  match ← StringBuilder.create values.size.toUSize dataSize.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildBoolColumn (values : Array Bool) : IO (Option ArrowArray) := do
  match ← BoolBuilder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildTimestampColumn (values : Array Int64) (timezone : String := "UTC") : IO (Option ArrowArray) := do
  match ← TimestampBuilder.create values.size.toUSize timezone with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildInt8Column (values : Array Int8) : IO (Option ArrowArray) := do
  match ← Int8Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildInt16Column (values : Array Int16) : IO (Option ArrowArray) := do
  match ← Int16Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildInt32Column (values : Array Int32) : IO (Option ArrowArray) := do
  match ← Int32Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildUInt8Column (values : Array UInt8) : IO (Option ArrowArray) := do
  match ← UInt8Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildUInt16Column (values : Array UInt16) : IO (Option ArrowArray) := do
  match ← UInt16Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildUInt32Column (values : Array UInt32) : IO (Option ArrowArray) := do
  match ← UInt32Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildUInt64Column (values : Array UInt64) : IO (Option ArrowArray) := do
  match ← UInt64Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildFloat32Column (values : Array Float) : IO (Option ArrowArray) := do
  match ← Float32Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildDate32Column (values : Array Int32) : IO (Option ArrowArray) := do
  match ← Date32Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildDate64Column (values : Array Int64) : IO (Option ArrowArray) := do
  match ← Date64Builder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildTime32Column (values : Array Int32) (unit : Time32Unit := .seconds) : IO (Option ArrowArray) := do
  match ← Time32Builder.create values.size.toUSize unit with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildTime64Column (values : Array Int64) (unit : Time64Unit := .microseconds) : IO (Option ArrowArray) := do
  match ← Time64Builder.create values.size.toUSize unit with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildDurationColumn (values : Array Int64) (unit : DurationUnit := .microseconds) : IO (Option ArrowArray) := do
  match ← DurationBuilder.create values.size.toUSize unit with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

def buildBinaryColumn (values : Array ByteArray) : IO (Option ArrowArray) := do
  let dataSize := values.foldl (fun acc b => acc + b.size) 0
  match ← BinaryBuilder.create values.size.toUSize dataSize.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

-- ============================================================================
-- List Builder (for List<T> arrays)
-- ============================================================================

structure ListBuilder where
  ptr : ListBuilderPtr.type
  elementType : ListElementType

namespace ListBuilder

def create (capacity : USize := 1024) (elementType : ListElementType := .int64) : IO (Option ListBuilder) := do
  match ← list_builder_create_impl capacity elementType.toUInt8 with
  | some ptr => return some ⟨ptr, elementType⟩
  | none => return none

def startList (builder : ListBuilder) : IO Bool :=
  list_builder_start_list_impl builder.ptr

def finishList (builder : ListBuilder) : IO Bool :=
  list_builder_finish_list_impl builder.ptr

def appendNull (builder : ListBuilder) : IO Bool :=
  list_builder_append_null_impl builder.ptr

def appendInt64 (builder : ListBuilder) (value : Int64) : IO Bool :=
  list_builder_append_int64_impl builder.ptr value

def appendFloat64 (builder : ListBuilder) (value : Float) : IO Bool :=
  list_builder_append_float64_impl builder.ptr value

def appendString (builder : ListBuilder) (value : String) : IO Bool :=
  list_builder_append_string_impl builder.ptr value

def appendBool (builder : ListBuilder) (value : Bool) : IO Bool :=
  list_builder_append_bool_impl builder.ptr (if value then 1 else 0)

/-- Append a complete list of Int64 values -/
def appendInt64List (builder : ListBuilder) (values : Array Int64) : IO Bool := do
  let ok ← builder.startList
  if !ok then return false
  for v in values do
    let ok ← builder.appendInt64 v
    if !ok then return false
  builder.finishList

/-- Append a complete list of Float64 values -/
def appendFloat64List (builder : ListBuilder) (values : Array Float) : IO Bool := do
  let ok ← builder.startList
  if !ok then return false
  for v in values do
    let ok ← builder.appendFloat64 v
    if !ok then return false
  builder.finishList

/-- Append a complete list of String values -/
def appendStringList (builder : ListBuilder) (values : Array String) : IO Bool := do
  let ok ← builder.startList
  if !ok then return false
  for v in values do
    let ok ← builder.appendString v
    if !ok then return false
  builder.finishList

def length (builder : ListBuilder) : IO USize :=
  list_builder_length_impl builder.ptr

def finish (builder : ListBuilder) : IO (Option ArrowArray) := do
  match ← list_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : ListBuilder) : IO Unit :=
  list_builder_free_impl builder.ptr

end ListBuilder

-- ============================================================================
-- Struct Builder (for Struct arrays)
-- ============================================================================

structure StructBuilder where
  ptr : StructBuilderPtr.type

namespace StructBuilder

def create (capacity : USize := 1024) : IO (Option StructBuilder) := do
  match ← struct_builder_create_impl capacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def addField (builder : StructBuilder) (name : String) (fieldType : ListElementType) : IO Bool :=
  struct_builder_add_field_impl builder.ptr name fieldType.toUInt8

def appendInt64 (builder : StructBuilder) (fieldIdx : USize) (value : Int64) : IO Bool :=
  struct_builder_append_int64_impl builder.ptr fieldIdx value

def appendFloat64 (builder : StructBuilder) (fieldIdx : USize) (value : Float) : IO Bool :=
  struct_builder_append_float64_impl builder.ptr fieldIdx value

def appendString (builder : StructBuilder) (fieldIdx : USize) (value : String) : IO Bool :=
  struct_builder_append_string_impl builder.ptr fieldIdx value

def appendBool (builder : StructBuilder) (fieldIdx : USize) (value : Bool) : IO Bool :=
  struct_builder_append_bool_impl builder.ptr fieldIdx (if value then 1 else 0)

def appendFieldNull (builder : StructBuilder) (fieldIdx : USize) : IO Bool :=
  struct_builder_append_field_null_impl builder.ptr fieldIdx

def appendNull (builder : StructBuilder) : IO Bool :=
  struct_builder_append_null_impl builder.ptr

def finishRow (builder : StructBuilder) : IO Bool :=
  struct_builder_finish_row_impl builder.ptr

def length (builder : StructBuilder) : IO USize :=
  struct_builder_length_impl builder.ptr

def fieldCount (builder : StructBuilder) : IO USize :=
  struct_builder_field_count_impl builder.ptr

def finish (builder : StructBuilder) : IO (Option ArrowArray) := do
  match ← struct_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def getSchema (builder : StructBuilder) : IO (Option ArrowSchema) := do
  match ← struct_builder_get_schema_impl builder.ptr with
  | some ptr =>
    let format ← arrow_schema_get_format_impl ptr
    let name ← arrow_schema_get_name_impl ptr
    let flags ← arrow_schema_get_flags_impl ptr
    return some { ptr, format, name, flags }
  | none => return none

def free (builder : StructBuilder) : IO Unit :=
  struct_builder_free_impl builder.ptr

end StructBuilder

-- ============================================================================
-- Decimal128 Builder
-- ============================================================================

structure Decimal128Builder where
  ptr : Decimal128BuilderPtr.type
  precision : Int32
  scale : Int32

namespace Decimal128Builder

def create (capacity : USize := 1024) (precision : Int32 := 38) (scale : Int32 := 9) : IO (Option Decimal128Builder) := do
  match ← decimal128_builder_create_impl capacity precision scale with
  | some ptr => return some ⟨ptr, precision, scale⟩
  | none => return none

/-- Append a decimal value from high and low 64-bit parts -/
def append (builder : Decimal128Builder) (high : Int64) (low : UInt64) : IO Bool :=
  decimal128_builder_append_impl builder.ptr high low

/-- Append a decimal value from a string representation (e.g., "123.45") -/
def appendString (builder : Decimal128Builder) (value : String) : IO Bool :=
  decimal128_builder_append_string_impl builder.ptr value

def appendNull (builder : Decimal128Builder) : IO Bool :=
  decimal128_builder_append_null_impl builder.ptr

def appendOptionString (builder : Decimal128Builder) (value : Option String) : IO Bool := do
  match value with
  | some v => builder.appendString v
  | none => builder.appendNull

def length (builder : Decimal128Builder) : IO USize :=
  decimal128_builder_length_impl builder.ptr

def getPrecision (builder : Decimal128Builder) : IO Int32 :=
  decimal128_builder_precision_impl builder.ptr

def getScale (builder : Decimal128Builder) : IO Int32 :=
  decimal128_builder_scale_impl builder.ptr

def finish (builder : Decimal128Builder) : IO (Option ArrowArray) := do
  match ← decimal128_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : Decimal128Builder) : IO Unit :=
  decimal128_builder_free_impl builder.ptr

end Decimal128Builder

-- ============================================================================
-- Dictionary Builder (for dictionary-encoded strings)
-- ============================================================================

structure DictionaryBuilder where
  ptr : DictionaryBuilderPtr.type

namespace DictionaryBuilder

def create (capacity : USize := 1024) (dictCapacity : USize := 256) : IO (Option DictionaryBuilder) := do
  match ← dictionary_builder_create_impl capacity dictCapacity with
  | some ptr => return some ⟨ptr⟩
  | none => return none

def append (builder : DictionaryBuilder) (value : String) : IO Bool :=
  dictionary_builder_append_impl builder.ptr value

def appendNull (builder : DictionaryBuilder) : IO Bool :=
  dictionary_builder_append_null_impl builder.ptr

def appendOption (builder : DictionaryBuilder) (value : Option String) : IO Bool := do
  match value with
  | some v => builder.append v
  | none => builder.appendNull

def appendAll (builder : DictionaryBuilder) (values : Array String) : IO Bool := do
  for v in values do
    let ok ← builder.append v
    if !ok then return false
  return true

def length (builder : DictionaryBuilder) : IO USize :=
  dictionary_builder_length_impl builder.ptr

def dictSize (builder : DictionaryBuilder) : IO USize :=
  dictionary_builder_dict_size_impl builder.ptr

def finish (builder : DictionaryBuilder) : IO (Option ArrowArray) := do
  match ← dictionary_builder_finish_impl builder.ptr with
  | some ptr =>
    let len ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr, length := len, null_count := nullCount, offset }
  | none => return none

def free (builder : DictionaryBuilder) : IO Unit :=
  dictionary_builder_free_impl builder.ptr

end DictionaryBuilder

-- ============================================================================
-- Convenience: Build nested columns
-- ============================================================================

/-- Build a List<Int64> column from arrays of arrays -/
def buildListInt64Column (values : Array (Array Int64)) : IO (Option ArrowArray) := do
  match ← ListBuilder.create values.size.toUSize .int64 with
  | some builder =>
    for arr in values do
      let _ ← builder.appendInt64List arr
    builder.finish
  | none => return none

/-- Build a List<Float64> column from arrays of arrays -/
def buildListFloat64Column (values : Array (Array Float)) : IO (Option ArrowArray) := do
  match ← ListBuilder.create values.size.toUSize .float64 with
  | some builder =>
    for arr in values do
      let _ ← builder.appendFloat64List arr
    builder.finish
  | none => return none

/-- Build a List<String> column from arrays of arrays -/
def buildListStringColumn (values : Array (Array String)) : IO (Option ArrowArray) := do
  match ← ListBuilder.create values.size.toUSize .string with
  | some builder =>
    for arr in values do
      let _ ← builder.appendStringList arr
    builder.finish
  | none => return none

/-- Build a dictionary-encoded string column -/
def buildDictionaryStringColumn (values : Array String) : IO (Option ArrowArray) := do
  match ← DictionaryBuilder.create values.size.toUSize with
  | some builder =>
    let _ ← builder.appendAll values
    builder.finish
  | none => return none

/-- Build a Decimal128 column from string values -/
def buildDecimal128Column (values : Array String) (precision : Int32 := 38) (scale : Int32 := 9) : IO (Option ArrowArray) := do
  match ← Decimal128Builder.create values.size.toUSize precision scale with
  | some builder =>
    for v in values do
      let _ ← builder.appendString v
    builder.finish
  | none => return none
