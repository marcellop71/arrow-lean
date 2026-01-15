-- Key Arrow C functions to wrap

import ArrowLean.Ops

-- External C functions that work with raw pointers
@[extern "lean_arrow_schema_init"]
opaque arrow_schema_init_impl (format: @& String) : IO ArrowSchemaPtr.type

@[extern "lean_arrow_schema_add_child"] 
opaque arrow_schema_add_child_impl (schema: @& ArrowSchemaPtr.type) (child: @& ArrowSchemaPtr.type) : IO Unit

@[extern "lean_arrow_schema_release"]
opaque arrow_schema_release_impl (schema: @& ArrowSchemaPtr.type) : IO Unit

@[extern "lean_arrow_schema_get_format"]
opaque arrow_schema_get_format_impl (schema: @& ArrowSchemaPtr.type) : IO String

@[extern "lean_arrow_schema_get_name"]
opaque arrow_schema_get_name_impl (schema: @& ArrowSchemaPtr.type) : IO (Option String)

@[extern "lean_arrow_schema_get_flags"]
opaque arrow_schema_get_flags_impl (schema: @& ArrowSchemaPtr.type) : IO UInt64

-- Schema introspection FFI
@[extern "lean_arrow_schema_get_child_count"]
opaque arrow_schema_get_child_count_impl (schema: @& ArrowSchemaPtr.type) : IO UInt64

@[extern "lean_arrow_schema_get_child"]
opaque arrow_schema_get_child_impl (schema: @& ArrowSchemaPtr.type) (index: UInt64) : IO (Option ArrowSchemaPtr.type)

@[extern "lean_arrow_schema_is_nullable"]
opaque arrow_schema_is_nullable_impl (schema: @& ArrowSchemaPtr.type) : IO Bool

@[extern "lean_arrow_schema_is_dictionary_ordered"]
opaque arrow_schema_is_dictionary_ordered_impl (schema: @& ArrowSchemaPtr.type) : IO Bool

@[extern "lean_arrow_schema_is_map_keys_sorted"]
opaque arrow_schema_is_map_keys_sorted_impl (schema: @& ArrowSchemaPtr.type) : IO Bool

@[extern "lean_arrow_schema_get_dictionary"]
opaque arrow_schema_get_dictionary_impl (schema: @& ArrowSchemaPtr.type) : IO (Option ArrowSchemaPtr.type)

@[extern "lean_arrow_schema_set_name"]
opaque arrow_schema_set_name_impl (schema: @& ArrowSchemaPtr.type) (name: @& String) : IO Unit

@[extern "lean_arrow_schema_set_flags"]
opaque arrow_schema_set_flags_impl (schema: @& ArrowSchemaPtr.type) (flags: UInt64) : IO Unit

-- High-level Schema Operations
def ArrowSchema.init (format: String) : IO ArrowSchema := do
  let ptr ← arrow_schema_init_impl format
  let name ← arrow_schema_get_name_impl ptr
  let flags ← arrow_schema_get_flags_impl ptr
  return { ptr := ptr, format := format, name := name, flags := flags }

def ArrowSchema.addChild (schema: ArrowSchema) (child: ArrowSchema) : IO Unit :=
  arrow_schema_add_child_impl schema.ptr child.ptr

def ArrowSchema.release (schema: ArrowSchema) : IO Unit :=
  arrow_schema_release_impl schema.ptr

-- Schema introspection high-level operations
/-- Get the number of child schemas -/
def ArrowSchema.getChildCount (schema: ArrowSchema) : IO UInt64 :=
  arrow_schema_get_child_count_impl schema.ptr

/-- Get a child schema by index -/
def ArrowSchema.getChild (schema: ArrowSchema) (index: UInt64) : IO (Option ArrowSchema) := do
  let opt_ptr ← arrow_schema_get_child_impl schema.ptr index
  match opt_ptr with
  | none => return none
  | some ptr => do
    let format ← arrow_schema_get_format_impl ptr
    let name ← arrow_schema_get_name_impl ptr
    let flags ← arrow_schema_get_flags_impl ptr
    return some { ptr := ptr, format := format, name := name, flags := flags }

/-- Check if the schema is nullable -/
def ArrowSchema.isNullable (schema: ArrowSchema) : IO Bool :=
  arrow_schema_is_nullable_impl schema.ptr

/-- Check if the dictionary is ordered (for dictionary-encoded types) -/
def ArrowSchema.isDictionaryOrdered (schema: ArrowSchema) : IO Bool :=
  arrow_schema_is_dictionary_ordered_impl schema.ptr

/-- Check if map keys are sorted (for map types) -/
def ArrowSchema.isMapKeysSorted (schema: ArrowSchema) : IO Bool :=
  arrow_schema_is_map_keys_sorted_impl schema.ptr

/-- Get the dictionary schema (for dictionary-encoded types) -/
def ArrowSchema.getDictionary (schema: ArrowSchema) : IO (Option ArrowSchema) := do
  let opt_ptr ← arrow_schema_get_dictionary_impl schema.ptr
  match opt_ptr with
  | none => return none
  | some ptr => do
    let format ← arrow_schema_get_format_impl ptr
    let name ← arrow_schema_get_name_impl ptr
    let flags ← arrow_schema_get_flags_impl ptr
    return some { ptr := ptr, format := format, name := name, flags := flags }

/-- Set the name of the schema -/
def ArrowSchema.setName (schema: ArrowSchema) (name: String) : IO Unit :=
  arrow_schema_set_name_impl schema.ptr name

/-- Set the flags of the schema -/
def ArrowSchema.setFlags (schema: ArrowSchema) (flags: UInt64) : IO Unit :=
  arrow_schema_set_flags_impl schema.ptr flags

/-- Get all child schemas -/
def ArrowSchema.getChildren (schema: ArrowSchema) : IO (Array ArrowSchema) := do
  let count ← schema.getChildCount
  let mut children := #[]
  for i in [0:count.toNat] do
    if let some child ← schema.getChild i.toUInt64 then
      children := children.push child
  return children

-- External Array C functions
@[extern "lean_arrow_array_init"]
opaque arrow_array_init_impl (length: UInt64) : IO ArrowArrayPtr.type

@[extern "lean_arrow_array_set_buffer"]
opaque arrow_array_set_buffer_impl (array: @& ArrowArrayPtr.type) (index: USize) (buffer: @& ArrowBufferPtr.type) : IO Unit

@[extern "lean_arrow_array_get_buffer"]
opaque arrow_array_get_buffer_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO ArrowBufferPtr.type

@[extern "lean_arrow_array_release"]
opaque arrow_array_release_impl (array: @& ArrowArrayPtr.type) : IO Unit

@[extern "lean_arrow_array_get_length"]
opaque arrow_array_get_length_impl (array: @& ArrowArrayPtr.type) : IO UInt64

@[extern "lean_arrow_array_get_null_count"]
opaque arrow_array_get_null_count_impl (array: @& ArrowArrayPtr.type) : IO UInt64

@[extern "lean_arrow_array_get_offset"]
opaque arrow_array_get_offset_impl (array: @& ArrowArrayPtr.type) : IO UInt64

-- Array introspection FFI
@[extern "lean_arrow_array_get_child_count"]
opaque arrow_array_get_child_count_impl (array: @& ArrowArrayPtr.type) : IO UInt64

@[extern "lean_arrow_array_get_child"]
opaque arrow_array_get_child_impl (array: @& ArrowArrayPtr.type) (index: UInt64) : IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_array_get_buffer_count"]
opaque arrow_array_get_buffer_count_impl (array: @& ArrowArrayPtr.type) : IO UInt64

@[extern "lean_arrow_array_get_dictionary"]
opaque arrow_array_get_dictionary_impl (array: @& ArrowArrayPtr.type) : IO (Option ArrowArrayPtr.type)

-- High-level Array Operations  
def ArrowArray.init (length: UInt64) : IO ArrowArray := do
  let ptr ← arrow_array_init_impl length
  let null_count ← arrow_array_get_null_count_impl ptr
  let offset ← arrow_array_get_offset_impl ptr
  return { ptr := ptr, length := length, null_count := null_count, offset := offset }

def ArrowArray.setBuffer (array: ArrowArray) (index: USize) (buffer: ArrowBufferPtr.type) : IO Unit :=
  arrow_array_set_buffer_impl array.ptr index buffer

def ArrowArray.getBuffer (array: ArrowArray) (index: USize) : IO ArrowBufferPtr.type :=
  arrow_array_get_buffer_impl array.ptr index

def ArrowArray.release (array: ArrowArray) : IO Unit :=
  arrow_array_release_impl array.ptr

-- Array introspection high-level operations
/-- Get the number of child arrays -/
def ArrowArray.getChildCount (array: ArrowArray) : IO UInt64 :=
  arrow_array_get_child_count_impl array.ptr

/-- Get a child array by index -/
def ArrowArray.getChild (array: ArrowArray) (index: UInt64) : IO (Option ArrowArray) := do
  let opt_ptr ← arrow_array_get_child_impl array.ptr index
  match opt_ptr with
  | none => return none
  | some ptr => do
    let length ← arrow_array_get_length_impl ptr
    let null_count ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr := ptr, length := length, null_count := null_count, offset := offset }

/-- Get the number of buffers in the array -/
def ArrowArray.getBufferCount (array: ArrowArray) : IO UInt64 :=
  arrow_array_get_buffer_count_impl array.ptr

/-- Get the dictionary array (for dictionary-encoded arrays) -/
def ArrowArray.getDictionary (array: ArrowArray) : IO (Option ArrowArray) := do
  let opt_ptr ← arrow_array_get_dictionary_impl array.ptr
  match opt_ptr with
  | none => return none
  | some ptr => do
    let length ← arrow_array_get_length_impl ptr
    let null_count ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr := ptr, length := length, null_count := null_count, offset := offset }

/-- Get all child arrays -/
def ArrowArray.getChildren (array: ArrowArray) : IO (Array ArrowArray) := do
  let count ← array.getChildCount
  let mut children := #[]
  for i in [0:count.toNat] do
    if let some child ← array.getChild i.toUInt64 then
      children := children.push child
  return children

-- External Stream C functions
@[extern "lean_arrow_stream_init"]
opaque arrow_stream_init_impl : IO ArrowArrayStreamPtr.type

@[extern "lean_arrow_stream_get_schema"]
opaque arrow_stream_get_schema_impl (stream: @& ArrowArrayStreamPtr.type) : IO (Option ArrowSchemaPtr.type)

@[extern "lean_arrow_stream_get_next"]
opaque arrow_stream_get_next_impl (stream: @& ArrowArrayStreamPtr.type) : IO (Option ArrowArrayPtr.type)

-- High-level Stream Operations
def ArrowArrayStream.init : IO ArrowArrayStream := do
  let ptr ← arrow_stream_init_impl
  return { ptr := ptr }

def ArrowArrayStream.getSchema (stream: ArrowArrayStream) : IO (Option ArrowSchema) := do
  let opt_ptr ← arrow_stream_get_schema_impl stream.ptr
  match opt_ptr with
  | none => return none
  | some ptr => do
    let format ← arrow_schema_get_format_impl ptr
    let name ← arrow_schema_get_name_impl ptr
    let flags ← arrow_schema_get_flags_impl ptr
    return some { ptr := ptr, format := format, name := name, flags := flags }

def ArrowArrayStream.getNext (stream: ArrowArrayStream) : IO (Option ArrowArray) := do
  let opt_ptr ← arrow_stream_get_next_impl stream.ptr
  match opt_ptr with
  | none => return none
  | some ptr => do
    let length ← arrow_array_get_length_impl ptr
    let null_count ← arrow_array_get_null_count_impl ptr
    let offset ← arrow_array_get_offset_impl ptr
    return some { ptr := ptr, length := length, null_count := null_count, offset := offset }

-- External Data Access C functions
@[extern "lean_arrow_get_bool_value"]
opaque arrow_get_bool_value_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO Bool

@[extern "lean_arrow_is_bool_null"]
opaque arrow_is_bool_null_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO Bool

@[extern "lean_arrow_get_int64_value"] 
opaque arrow_get_int64_value_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO Int64

@[extern "lean_arrow_is_int64_null"]
opaque arrow_is_int64_null_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO Bool

@[extern "lean_arrow_get_string_value"]
opaque arrow_get_string_value_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO String

@[extern "lean_arrow_is_string_null"]
opaque arrow_is_string_null_impl (array: @& ArrowArrayPtr.type) (index: USize) : IO Bool

-- High-level Data Access Functions
def ArrowArray.getBool (array: ArrowArray) (index: USize) : IO (Option Bool) := do
  let is_null ← arrow_is_bool_null_impl array.ptr index
  if is_null then
    return none
  else
    let value ← arrow_get_bool_value_impl array.ptr index
    return some value

def ArrowArray.getInt64 (array: ArrowArray) (index: USize) : IO (Option Int64) := do
  let is_null ← arrow_is_int64_null_impl array.ptr index
  if is_null then
    return none
  else
    let value ← arrow_get_int64_value_impl array.ptr index
    return some value

def ArrowArray.getString (array: ArrowArray) (index: USize) : IO (Option String) := do
  let is_null ← arrow_is_string_null_impl array.ptr index
  if is_null then
    return none
  else
    let value ← arrow_get_string_value_impl array.ptr index
    return some value

-- External Buffer Management C functions
@[extern "lean_arrow_allocate_buffer"]
opaque arrow_allocate_buffer_impl (size: USize) : IO ArrowBufferPtr.type

@[extern "lean_arrow_buffer_resize"]
opaque arrow_buffer_resize_impl (buffer: @& ArrowBufferPtr.type) (new_size: USize) : IO Unit

@[extern "lean_arrow_buffer_free"]
opaque arrow_buffer_free_impl (buffer: @& ArrowBufferPtr.type) : IO Unit

@[extern "lean_arrow_buffer_get_size"]
opaque arrow_buffer_get_size_impl (buffer: @& ArrowBufferPtr.type) : IO USize

@[extern "lean_arrow_buffer_get_capacity"]
opaque arrow_buffer_get_capacity_impl (buffer: @& ArrowBufferPtr.type) : IO USize

-- High-level Buffer Management
def ArrowBuffer.allocate (size: USize) : IO ArrowBuffer := do
  let ptr ← arrow_allocate_buffer_impl size
  let capacity ← arrow_buffer_get_capacity_impl ptr
  return { ptr := ptr, size := size, capacity := capacity }

def ArrowBuffer.resize (buffer: ArrowBuffer) (new_size: USize) : IO Unit :=
  arrow_buffer_resize_impl buffer.ptr new_size

def ArrowBuffer.free (buffer: ArrowBuffer) : IO Unit :=
  arrow_buffer_free_impl buffer.ptr