-- Additional High-level wrapper functions

import ArrowLean.Ops
import ArrowLean.FFI

-- Utility functions for ArrowType
def TimeUnit.toFormatString : TimeUnit → String
  | TimeUnit.second => "s"
  | TimeUnit.millisecond => "m"
  | TimeUnit.microsecond => "u"
  | TimeUnit.nanosecond => "n"

def ArrowType.toString : ArrowType → String
  | ArrowType.null => "n"
  | ArrowType.boolean => "b"
  | ArrowType.int8 => "c"
  | ArrowType.int16 => "s"
  | ArrowType.int32 => "i"
  | ArrowType.int64 => "l"
  | ArrowType.uint8 => "C"
  | ArrowType.uint16 => "S"
  | ArrowType.uint32 => "I"
  | ArrowType.uint64 => "L"
  | ArrowType.float16 => "e"
  | ArrowType.float32 => "f"
  | ArrowType.float64 => "g"
  | ArrowType.string => "u"
  | ArrowType.binary => "z"
  | ArrowType.timestamp unit => s!"ts{TimeUnit.toFormatString unit}:UTC"
  | ArrowType.date32 => "tdD"
  | ArrowType.date64 => "tdm"
  | ArrowType.list _ => "+l"
  | ArrowType.struct _ => "+s"

-- Create a schema for a primitive type with a given name
def ArrowSchema.forType (dataType : ArrowType) (_name : String) : IO ArrowSchema := do
  let format := dataType.toString
  ArrowSchema.init format

-- Create a struct schema (for tables with multiple columns)
def ArrowSchema.struct (_name : String := "") : IO ArrowSchema := do
  ArrowSchema.init "+s"

-- Release an ArrowArrayStream
def ArrowArrayStream.release (_stream : ArrowArrayStream) : IO Unit := do
  -- Streams are managed by their source (reader), so we just clean up our reference
  pure ()

-- Iterate over all arrays in a stream
partial def ArrowArrayStream.forEachArray (stream : ArrowArrayStream) (f : ArrowArray → IO Unit) : IO Unit := do
  let opt ← stream.getNext
  match opt with
  | none => pure ()
  | some array => do
    f array
    array.release
    stream.forEachArray f

-- Collect all arrays from a stream (be careful with large datasets!)
partial def ArrowArrayStream.toArrays (stream : ArrowArrayStream) : IO (Array ArrowArray) := do
  let mut result := #[]
  let opt ← stream.getNext
  match opt with
  | none => return result
  | some array => do
    result := result.push array
    let rest ← stream.toArrays
    return result ++ rest

-- Get Float64 value from array (market data often uses Float64)
@[extern "lean_arrow_get_float64_value"]
opaque arrow_get_float64_value_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO Float

@[extern "lean_arrow_is_float64_null"]
opaque arrow_is_float64_null_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO Bool

def ArrowArray.getFloat64 (array: ArrowArray) (index: USize) : IO (Option Float) := do
  let is_null ← arrow_is_float64_null_impl array.ptr index
  if is_null then
    return none
  else
    let value ← arrow_get_float64_value_impl array.ptr index
    return some value

-- Get UInt64 value from array
@[extern "lean_arrow_get_uint64_value"]
opaque arrow_get_uint64_value_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO UInt64

@[extern "lean_arrow_is_uint64_null"]
opaque arrow_is_uint64_null_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO Bool

def ArrowArray.getUInt64 (array: ArrowArray) (index: USize) : IO (Option UInt64) := do
  let is_null ← arrow_is_uint64_null_impl array.ptr index
  if is_null then
    return none
  else
    let value ← arrow_get_uint64_value_impl array.ptr index
    return some value

-- Iterate over Int64 values in an array
def ArrowArray.forEachInt64 (array : ArrowArray) (f : USize → Option Int64 → IO Unit) : IO Unit := do
  let len := array.length.toUSize
  for i in [:len.toNat] do
    let idx := i.toUSize
    let value ← array.getInt64 idx
    f idx value

-- Iterate over Float64 values in an array
def ArrowArray.forEachFloat64 (array : ArrowArray) (f : USize → Option Float → IO Unit) : IO Unit := do
  let len := array.length.toUSize
  for i in [:len.toNat] do
    let idx := i.toUSize
    let value ← array.getFloat64 idx
    f idx value

-- Iterate over String values in an array
def ArrowArray.forEachString (array : ArrowArray) (f : USize → Option String → IO Unit) : IO Unit := do
  let len := array.length.toUSize
  for i in [:len.toNat] do
    let idx := i.toUSize
    let value ← array.getString idx
    f idx value