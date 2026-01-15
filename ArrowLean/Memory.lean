-- Memory management and finalization for Arrow objects

import ArrowLean.Ops
import ArrowLean.FFI

-- External finalization functions
@[extern "lean_arrow_schema_finalizer"]
opaque arrow_schema_finalizer : ArrowSchemaPtr.type → Unit

@[extern "lean_arrow_array_finalizer"]
opaque arrow_array_finalizer : ArrowArrayPtr.type → Unit

@[extern "lean_arrow_stream_finalizer"]
opaque arrow_stream_finalizer : ArrowArrayStreamPtr.type → Unit

@[extern "lean_arrow_buffer_finalizer"]
opaque arrow_buffer_finalizer : ArrowBufferPtr.type → Unit

-- Instances to enable automatic memory management
noncomputable instance : Inhabited ArrowSchemaPtr.type := ⟨Classical.choice ArrowSchemaPtr.property⟩

noncomputable instance : Inhabited ArrowArrayPtr.type := ⟨Classical.choice ArrowArrayPtr.property⟩

noncomputable instance : Inhabited ArrowArrayStreamPtr.type := ⟨Classical.choice ArrowArrayStreamPtr.property⟩

noncomputable instance : Inhabited ArrowBufferPtr.type := ⟨Classical.choice ArrowBufferPtr.property⟩

-- Helper functions for creating finalized objects
def ArrowSchema.mkManaged (ptr : ArrowSchemaPtr.type) (format : String) (name : Option String) (flags : UInt64) : ArrowSchema :=
  -- Note: Finalization will be handled through Lean's memory management
  { ptr := ptr, format := format, name := name, flags := flags }

def ArrowArray.mkManaged (ptr : ArrowArrayPtr.type) (length : UInt64) (null_count : UInt64) (offset : UInt64) : ArrowArray :=
  -- Note: Finalization will be handled through Lean's memory management
  { ptr := ptr, length := length, null_count := null_count, offset := offset }

def ArrowArrayStream.mkManaged (ptr : ArrowArrayStreamPtr.type) : ArrowArrayStream :=
  -- Note: Finalization will be handled through Lean's memory management
  { ptr := ptr }

def ArrowBuffer.mkManaged (ptr : ArrowBufferPtr.type) (size : USize) (capacity : USize) : ArrowBuffer :=
  -- Note: Finalization will be handled through Lean's memory management
  { ptr := ptr, size := size, capacity := capacity }

-- Updated constructors that use managed memory
def ArrowSchema.initManaged (format: String) : IO ArrowSchema := do
  let ptr ← arrow_schema_init_impl format
  let name ← arrow_schema_get_name_impl ptr
  let flags ← arrow_schema_get_flags_impl ptr
  return ArrowSchema.mkManaged ptr format name flags

def ArrowArray.initManaged (length: UInt64) : IO ArrowArray := do
  let ptr ← arrow_array_init_impl length
  let null_count ← arrow_array_get_null_count_impl ptr
  let offset ← arrow_array_get_offset_impl ptr
  return ArrowArray.mkManaged ptr length null_count offset

def ArrowArrayStream.initManaged : IO ArrowArrayStream := do
  let ptr ← arrow_stream_init_impl
  return ArrowArrayStream.mkManaged ptr

def ArrowBuffer.allocateManaged (size: USize) : IO ArrowBuffer := do
  let ptr ← arrow_allocate_buffer_impl size
  let capacity ← arrow_buffer_get_capacity_impl ptr
  return ArrowBuffer.mkManaged ptr size capacity