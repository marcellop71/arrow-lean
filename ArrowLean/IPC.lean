/-
  Arrow IPC Serialization/Deserialization

  Provides functions to serialize Arrow schemas, arrays, and record batches
  to binary format for storage (e.g., in Redis) and later retrieval.

  This implements a simplified binary format optimized for same-application
  round-trips, enabling patterns like:
  - Store Arrow batches in Redis as binary values
  - Serialize for network transmission
  - Cache Arrow data to disk
-/

import ArrowLean.Ops
import ArrowLean.FFI

namespace ArrowLean.IPC

/-! ## RecordBatch Type

A RecordBatch bundles a schema with its corresponding array data.
This is the primary unit for serialization/storage.
-/

/-- A RecordBatch contains both schema metadata and array data -/
structure RecordBatch where
  schema : ArrowSchema
  array : ArrowArray
  deriving Nonempty

namespace RecordBatch

/-- Get the number of rows in the batch -/
def length (batch : RecordBatch) : UInt64 :=
  batch.array.length

/-- Get the format string of the batch's schema -/
def format (batch : RecordBatch) : String :=
  batch.schema.format

/-- Release both schema and array resources -/
def release (batch : RecordBatch) : IO Unit := do
  ArrowSchema.release batch.schema
  ArrowArray.release batch.array

end RecordBatch

/-! ## FFI Declarations -/

/-- Serialize a schema to binary format -/
@[extern "lean_arrow_ipc_serialize_schema"]
opaque serializeSchemaRaw : @& ArrowSchemaPtr.type → IO ByteArray

/-- Deserialize binary data to a schema -/
@[extern "lean_arrow_ipc_deserialize_schema"]
opaque deserializeSchemaRaw : @& ByteArray → IO (Option ArrowSchemaPtr.type)

/-- Serialize an array to binary format (requires schema for type info) -/
@[extern "lean_arrow_ipc_serialize_array"]
opaque serializeArrayRaw : @& ArrowArrayPtr.type → @& ArrowSchemaPtr.type → IO ByteArray

/-- Deserialize binary data to an array -/
@[extern "lean_arrow_ipc_deserialize_array"]
opaque deserializeArrayRaw : @& ByteArray → @& ArrowSchemaPtr.type → IO (Option ArrowArrayPtr.type)

/-- Serialize a record batch (schema + array) to binary format -/
@[extern "lean_arrow_ipc_serialize_batch"]
opaque serializeBatchRaw : @& ArrowSchemaPtr.type → @& ArrowArrayPtr.type → IO ByteArray

/-- Deserialize binary data to a record batch -/
@[extern "lean_arrow_ipc_deserialize_batch"]
opaque deserializeBatchRaw : @& ByteArray → IO (Option (ArrowSchemaPtr.type × ArrowArrayPtr.type))

/-- Get the serialized size of a batch without full serialization -/
@[extern "lean_arrow_ipc_batch_size"]
opaque batchSerializedSizeRaw : @& ArrowSchemaPtr.type → @& ArrowArrayPtr.type → IO UInt64

/-! ## High-Level API -/

/-- Serialize an ArrowSchema to binary format -/
def serializeSchema (schema : ArrowSchema) : IO ByteArray :=
  serializeSchemaRaw schema.ptr

/-- Deserialize binary data to an ArrowSchema -/
def deserializeSchema (data : ByteArray) : IO (Option ArrowSchema) := do
  match ← deserializeSchemaRaw data with
  | none => return none
  | some ptr =>
    -- Get schema metadata from the deserialized pointer
    let format ← arrow_schema_get_format_impl ptr
    let name ← arrow_schema_get_name_impl ptr
    let flags ← arrow_schema_get_flags_impl ptr
    return some {
      ptr := ptr
      format := format
      name := name
      flags := flags
    }

/-- Serialize an ArrowArray to binary format -/
def serializeArray (array : ArrowArray) (schema : ArrowSchema) : IO ByteArray :=
  serializeArrayRaw array.ptr schema.ptr

/-- Deserialize binary data to an ArrowArray -/
def deserializeArray (data : ByteArray) (schema : ArrowSchema) : IO (Option ArrowArray) := do
  match ← deserializeArrayRaw data schema.ptr with
  | none => return none
  | some ptr =>
    let length ← arrow_array_get_length_impl ptr
    let nullCount ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some {
      ptr := ptr
      length := length
      null_count := nullCount
      offset := offset
    }

/-- Serialize a RecordBatch to binary format -/
def serialize (batch : RecordBatch) : IO ByteArray :=
  serializeBatchRaw batch.schema.ptr batch.array.ptr

/-- Deserialize binary data to a RecordBatch -/
def deserialize (data : ByteArray) : IO (Option RecordBatch) := do
  match ← deserializeBatchRaw data with
  | none => return none
  | some (schemaPtr, arrayPtr) =>
    -- Build schema
    let format ← arrow_schema_get_format_impl schemaPtr
    let name ← arrow_schema_get_name_impl schemaPtr
    let flags ← arrow_schema_get_flags_impl schemaPtr
    let schema : ArrowSchema := {
      ptr := schemaPtr
      format := format
      name := name
      flags := flags
    }
    -- Build array
    let length ← arrow_array_get_length_impl arrayPtr
    let nullCount ← arrow_array_get_null_count_impl arrayPtr
    let offset ← arrow_array_get_offset_impl arrayPtr
    let array : ArrowArray := {
      ptr := arrayPtr
      length := length
      null_count := nullCount
      offset := offset
    }
    return some { schema := schema, array := array }

/-- Get the serialized size of a RecordBatch -/
def serializedSize (batch : RecordBatch) : IO UInt64 :=
  batchSerializedSizeRaw batch.schema.ptr batch.array.ptr

/-! ## Convenience Functions -/

/-- Serialize a schema and array pair to binary format -/
def serializePair (schema : ArrowSchema) (array : ArrowArray) : IO ByteArray :=
  serializeBatchRaw schema.ptr array.ptr

/-- Round-trip test: serialize and deserialize, checking equality of length -/
def roundTrip (batch : RecordBatch) : IO (Option RecordBatch) := do
  let data ← serialize batch
  deserialize data

/-! ## Binary Format Information

The IPC format uses the following structure:

**RecordBatch:**
```
[4 bytes] Magic: "ARRB" (0x42525241)
[4 bytes] Version: 1
[... schema data ...]
[... array data ...]
```

**Schema:**
```
[4 bytes] Format string length
[N bytes] Format string (UTF-8)
[4 bytes] Name length (0 if none)
[N bytes] Name (UTF-8)
[8 bytes] Flags
[8 bytes] Number of children
[... child schemas recursively ...]
```

**Array:**
```
[8 bytes] Length
[8 bytes] Null count
[8 bytes] Offset
[8 bytes] Number of buffers
[8 bytes] Number of children
[... for each buffer: size + data ...]
[... child arrays recursively ...]
```
-/

end ArrowLean.IPC
