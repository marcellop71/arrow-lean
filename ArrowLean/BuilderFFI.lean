-- Low-level FFI bindings for typed Arrow builders

import ArrowLean.Ops

-- ============================================================================
-- Opaque Builder Pointer Types
-- ============================================================================

opaque Int8BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque Int16BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque Int32BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque Int64BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque UInt8BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque UInt16BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque UInt32BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque UInt64BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque Float32BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque Float64BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque StringBuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque TimestampBuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque BoolBuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque Date32BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque Date64BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque Time32BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque Time64BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque DurationBuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque BinaryBuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque SchemaBuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque RecordBatchPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩

-- Nested type builder pointers
opaque ListBuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque StructBuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque Decimal128BuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque DictionaryBuilderPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩

instance : Nonempty Int8BuilderPtr.type := Int8BuilderPtr.property
instance : Nonempty Int16BuilderPtr.type := Int16BuilderPtr.property
instance : Nonempty Int32BuilderPtr.type := Int32BuilderPtr.property
instance : Nonempty Int64BuilderPtr.type := Int64BuilderPtr.property
instance : Nonempty UInt8BuilderPtr.type := UInt8BuilderPtr.property
instance : Nonempty UInt16BuilderPtr.type := UInt16BuilderPtr.property
instance : Nonempty UInt32BuilderPtr.type := UInt32BuilderPtr.property
instance : Nonempty UInt64BuilderPtr.type := UInt64BuilderPtr.property
instance : Nonempty Float32BuilderPtr.type := Float32BuilderPtr.property
instance : Nonempty Float64BuilderPtr.type := Float64BuilderPtr.property
instance : Nonempty StringBuilderPtr.type := StringBuilderPtr.property
instance : Nonempty TimestampBuilderPtr.type := TimestampBuilderPtr.property
instance : Nonempty BoolBuilderPtr.type := BoolBuilderPtr.property
instance : Nonempty Date32BuilderPtr.type := Date32BuilderPtr.property
instance : Nonempty Date64BuilderPtr.type := Date64BuilderPtr.property
instance : Nonempty Time32BuilderPtr.type := Time32BuilderPtr.property
instance : Nonempty Time64BuilderPtr.type := Time64BuilderPtr.property
instance : Nonempty DurationBuilderPtr.type := DurationBuilderPtr.property
instance : Nonempty BinaryBuilderPtr.type := BinaryBuilderPtr.property
instance : Nonempty SchemaBuilderPtr.type := SchemaBuilderPtr.property
instance : Nonempty RecordBatchPtr.type := RecordBatchPtr.property
instance : Nonempty ListBuilderPtr.type := ListBuilderPtr.property
instance : Nonempty StructBuilderPtr.type := StructBuilderPtr.property
instance : Nonempty Decimal128BuilderPtr.type := Decimal128BuilderPtr.property
instance : Nonempty DictionaryBuilderPtr.type := DictionaryBuilderPtr.property

-- ============================================================================
-- Int8 Builder FFI
-- ============================================================================

@[extern "lean_int8_builder_create"]
opaque int8_builder_create_impl (capacity : USize) : IO (Option Int8BuilderPtr.type)

@[extern "lean_int8_builder_append"]
opaque int8_builder_append_impl (builder : @& Int8BuilderPtr.type) (value : Int8) : IO Bool

@[extern "lean_int8_builder_append_null"]
opaque int8_builder_append_null_impl (builder : @& Int8BuilderPtr.type) : IO Bool

@[extern "lean_int8_builder_finish"]
opaque int8_builder_finish_impl (builder : @& Int8BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_int8_builder_free"]
opaque int8_builder_free_impl (builder : @& Int8BuilderPtr.type) : IO Unit

@[extern "lean_int8_builder_length"]
opaque int8_builder_length_impl (builder : @& Int8BuilderPtr.type) : IO USize

-- ============================================================================
-- Int16 Builder FFI
-- ============================================================================

@[extern "lean_int16_builder_create"]
opaque int16_builder_create_impl (capacity : USize) : IO (Option Int16BuilderPtr.type)

@[extern "lean_int16_builder_append"]
opaque int16_builder_append_impl (builder : @& Int16BuilderPtr.type) (value : Int16) : IO Bool

@[extern "lean_int16_builder_append_null"]
opaque int16_builder_append_null_impl (builder : @& Int16BuilderPtr.type) : IO Bool

@[extern "lean_int16_builder_finish"]
opaque int16_builder_finish_impl (builder : @& Int16BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_int16_builder_free"]
opaque int16_builder_free_impl (builder : @& Int16BuilderPtr.type) : IO Unit

@[extern "lean_int16_builder_length"]
opaque int16_builder_length_impl (builder : @& Int16BuilderPtr.type) : IO USize

-- ============================================================================
-- Int32 Builder FFI
-- ============================================================================

@[extern "lean_int32_builder_create"]
opaque int32_builder_create_impl (capacity : USize) : IO (Option Int32BuilderPtr.type)

@[extern "lean_int32_builder_append"]
opaque int32_builder_append_impl (builder : @& Int32BuilderPtr.type) (value : Int32) : IO Bool

@[extern "lean_int32_builder_append_null"]
opaque int32_builder_append_null_impl (builder : @& Int32BuilderPtr.type) : IO Bool

@[extern "lean_int32_builder_finish"]
opaque int32_builder_finish_impl (builder : @& Int32BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_int32_builder_free"]
opaque int32_builder_free_impl (builder : @& Int32BuilderPtr.type) : IO Unit

@[extern "lean_int32_builder_length"]
opaque int32_builder_length_impl (builder : @& Int32BuilderPtr.type) : IO USize

-- ============================================================================
-- Int64 Builder FFI
-- ============================================================================

@[extern "lean_int64_builder_create"]
opaque int64_builder_create_impl (capacity : USize) : IO (Option Int64BuilderPtr.type)

@[extern "lean_int64_builder_append"]
opaque int64_builder_append_impl (builder : @& Int64BuilderPtr.type) (value : Int64) : IO Bool

@[extern "lean_int64_builder_append_null"]
opaque int64_builder_append_null_impl (builder : @& Int64BuilderPtr.type) : IO Bool

@[extern "lean_int64_builder_finish"]
opaque int64_builder_finish_impl (builder : @& Int64BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_int64_builder_free"]
opaque int64_builder_free_impl (builder : @& Int64BuilderPtr.type) : IO Unit

@[extern "lean_int64_builder_length"]
opaque int64_builder_length_impl (builder : @& Int64BuilderPtr.type) : IO USize

-- ============================================================================
-- Float64 Builder FFI
-- ============================================================================

@[extern "lean_float64_builder_create"]
opaque float64_builder_create_impl (capacity : USize) : IO (Option Float64BuilderPtr.type)

@[extern "lean_float64_builder_append"]
opaque float64_builder_append_impl (builder : @& Float64BuilderPtr.type) (value : Float) : IO Bool

@[extern "lean_float64_builder_append_null"]
opaque float64_builder_append_null_impl (builder : @& Float64BuilderPtr.type) : IO Bool

@[extern "lean_float64_builder_finish"]
opaque float64_builder_finish_impl (builder : @& Float64BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_float64_builder_free"]
opaque float64_builder_free_impl (builder : @& Float64BuilderPtr.type) : IO Unit

@[extern "lean_float64_builder_length"]
opaque float64_builder_length_impl (builder : @& Float64BuilderPtr.type) : IO USize

-- ============================================================================
-- String Builder FFI
-- ============================================================================

@[extern "lean_string_builder_create"]
opaque string_builder_create_impl (capacity : USize) (dataCapacity : USize) : IO (Option StringBuilderPtr.type)

@[extern "lean_string_builder_append"]
opaque string_builder_append_impl (builder : @& StringBuilderPtr.type) (value : @& String) : IO Bool

@[extern "lean_string_builder_append_null"]
opaque string_builder_append_null_impl (builder : @& StringBuilderPtr.type) : IO Bool

@[extern "lean_string_builder_finish"]
opaque string_builder_finish_impl (builder : @& StringBuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_string_builder_free"]
opaque string_builder_free_impl (builder : @& StringBuilderPtr.type) : IO Unit

@[extern "lean_string_builder_length"]
opaque string_builder_length_impl (builder : @& StringBuilderPtr.type) : IO USize

-- ============================================================================
-- Timestamp Builder FFI
-- ============================================================================

@[extern "lean_timestamp_builder_create"]
opaque timestamp_builder_create_impl (capacity : USize) (timezone : @& String) : IO (Option TimestampBuilderPtr.type)

@[extern "lean_timestamp_builder_append"]
opaque timestamp_builder_append_impl (builder : @& TimestampBuilderPtr.type) (microseconds : Int64) : IO Bool

@[extern "lean_timestamp_builder_append_null"]
opaque timestamp_builder_append_null_impl (builder : @& TimestampBuilderPtr.type) : IO Bool

@[extern "lean_timestamp_builder_finish"]
opaque timestamp_builder_finish_impl (builder : @& TimestampBuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_timestamp_builder_free"]
opaque timestamp_builder_free_impl (builder : @& TimestampBuilderPtr.type) : IO Unit

@[extern "lean_timestamp_builder_length"]
opaque timestamp_builder_length_impl (builder : @& TimestampBuilderPtr.type) : IO USize

@[extern "lean_timestamp_builder_get_timezone"]
opaque timestamp_builder_get_timezone_impl (builder : @& TimestampBuilderPtr.type) : IO String

-- ============================================================================
-- Bool Builder FFI
-- ============================================================================

@[extern "lean_bool_builder_create"]
opaque bool_builder_create_impl (capacity : USize) : IO (Option BoolBuilderPtr.type)

@[extern "lean_bool_builder_append"]
opaque bool_builder_append_impl (builder : @& BoolBuilderPtr.type) (value : UInt8) : IO Bool

@[extern "lean_bool_builder_append_null"]
opaque bool_builder_append_null_impl (builder : @& BoolBuilderPtr.type) : IO Bool

@[extern "lean_bool_builder_finish"]
opaque bool_builder_finish_impl (builder : @& BoolBuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_bool_builder_free"]
opaque bool_builder_free_impl (builder : @& BoolBuilderPtr.type) : IO Unit

@[extern "lean_bool_builder_length"]
opaque bool_builder_length_impl (builder : @& BoolBuilderPtr.type) : IO USize

-- ============================================================================
-- UInt8 Builder FFI
-- ============================================================================

@[extern "lean_uint8_builder_create"]
opaque uint8_builder_create_impl (capacity : USize) : IO (Option UInt8BuilderPtr.type)

@[extern "lean_uint8_builder_append"]
opaque uint8_builder_append_impl (builder : @& UInt8BuilderPtr.type) (value : UInt8) : IO Bool

@[extern "lean_uint8_builder_append_null"]
opaque uint8_builder_append_null_impl (builder : @& UInt8BuilderPtr.type) : IO Bool

@[extern "lean_uint8_builder_finish"]
opaque uint8_builder_finish_impl (builder : @& UInt8BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_uint8_builder_free"]
opaque uint8_builder_free_impl (builder : @& UInt8BuilderPtr.type) : IO Unit

@[extern "lean_uint8_builder_length"]
opaque uint8_builder_length_impl (builder : @& UInt8BuilderPtr.type) : IO USize

-- ============================================================================
-- UInt16 Builder FFI
-- ============================================================================

@[extern "lean_uint16_builder_create"]
opaque uint16_builder_create_impl (capacity : USize) : IO (Option UInt16BuilderPtr.type)

@[extern "lean_uint16_builder_append"]
opaque uint16_builder_append_impl (builder : @& UInt16BuilderPtr.type) (value : UInt16) : IO Bool

@[extern "lean_uint16_builder_append_null"]
opaque uint16_builder_append_null_impl (builder : @& UInt16BuilderPtr.type) : IO Bool

@[extern "lean_uint16_builder_finish"]
opaque uint16_builder_finish_impl (builder : @& UInt16BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_uint16_builder_free"]
opaque uint16_builder_free_impl (builder : @& UInt16BuilderPtr.type) : IO Unit

@[extern "lean_uint16_builder_length"]
opaque uint16_builder_length_impl (builder : @& UInt16BuilderPtr.type) : IO USize

-- ============================================================================
-- UInt32 Builder FFI
-- ============================================================================

@[extern "lean_uint32_builder_create"]
opaque uint32_builder_create_impl (capacity : USize) : IO (Option UInt32BuilderPtr.type)

@[extern "lean_uint32_builder_append"]
opaque uint32_builder_append_impl (builder : @& UInt32BuilderPtr.type) (value : UInt32) : IO Bool

@[extern "lean_uint32_builder_append_null"]
opaque uint32_builder_append_null_impl (builder : @& UInt32BuilderPtr.type) : IO Bool

@[extern "lean_uint32_builder_finish"]
opaque uint32_builder_finish_impl (builder : @& UInt32BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_uint32_builder_free"]
opaque uint32_builder_free_impl (builder : @& UInt32BuilderPtr.type) : IO Unit

@[extern "lean_uint32_builder_length"]
opaque uint32_builder_length_impl (builder : @& UInt32BuilderPtr.type) : IO USize

-- ============================================================================
-- UInt64 Builder FFI
-- ============================================================================

@[extern "lean_uint64_builder_create"]
opaque uint64_builder_create_impl (capacity : USize) : IO (Option UInt64BuilderPtr.type)

@[extern "lean_uint64_builder_append"]
opaque uint64_builder_append_impl (builder : @& UInt64BuilderPtr.type) (value : UInt64) : IO Bool

@[extern "lean_uint64_builder_append_null"]
opaque uint64_builder_append_null_impl (builder : @& UInt64BuilderPtr.type) : IO Bool

@[extern "lean_uint64_builder_finish"]
opaque uint64_builder_finish_impl (builder : @& UInt64BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_uint64_builder_free"]
opaque uint64_builder_free_impl (builder : @& UInt64BuilderPtr.type) : IO Unit

@[extern "lean_uint64_builder_length"]
opaque uint64_builder_length_impl (builder : @& UInt64BuilderPtr.type) : IO USize

-- ============================================================================
-- Float32 Builder FFI
-- ============================================================================

@[extern "lean_float32_builder_create"]
opaque float32_builder_create_impl (capacity : USize) : IO (Option Float32BuilderPtr.type)

@[extern "lean_float32_builder_append"]
opaque float32_builder_append_impl (builder : @& Float32BuilderPtr.type) (value : Float) : IO Bool

@[extern "lean_float32_builder_append_null"]
opaque float32_builder_append_null_impl (builder : @& Float32BuilderPtr.type) : IO Bool

@[extern "lean_float32_builder_finish"]
opaque float32_builder_finish_impl (builder : @& Float32BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_float32_builder_free"]
opaque float32_builder_free_impl (builder : @& Float32BuilderPtr.type) : IO Unit

@[extern "lean_float32_builder_length"]
opaque float32_builder_length_impl (builder : @& Float32BuilderPtr.type) : IO USize

-- ============================================================================
-- Date32 Builder FFI
-- ============================================================================

@[extern "lean_date32_builder_create"]
opaque date32_builder_create_impl (capacity : USize) : IO (Option Date32BuilderPtr.type)

@[extern "lean_date32_builder_append"]
opaque date32_builder_append_impl (builder : @& Date32BuilderPtr.type) (value : Int32) : IO Bool

@[extern "lean_date32_builder_append_null"]
opaque date32_builder_append_null_impl (builder : @& Date32BuilderPtr.type) : IO Bool

@[extern "lean_date32_builder_finish"]
opaque date32_builder_finish_impl (builder : @& Date32BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_date32_builder_free"]
opaque date32_builder_free_impl (builder : @& Date32BuilderPtr.type) : IO Unit

@[extern "lean_date32_builder_length"]
opaque date32_builder_length_impl (builder : @& Date32BuilderPtr.type) : IO USize

-- ============================================================================
-- Date64 Builder FFI
-- ============================================================================

@[extern "lean_date64_builder_create"]
opaque date64_builder_create_impl (capacity : USize) : IO (Option Date64BuilderPtr.type)

@[extern "lean_date64_builder_append"]
opaque date64_builder_append_impl (builder : @& Date64BuilderPtr.type) (value : Int64) : IO Bool

@[extern "lean_date64_builder_append_null"]
opaque date64_builder_append_null_impl (builder : @& Date64BuilderPtr.type) : IO Bool

@[extern "lean_date64_builder_finish"]
opaque date64_builder_finish_impl (builder : @& Date64BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_date64_builder_free"]
opaque date64_builder_free_impl (builder : @& Date64BuilderPtr.type) : IO Unit

@[extern "lean_date64_builder_length"]
opaque date64_builder_length_impl (builder : @& Date64BuilderPtr.type) : IO USize

-- ============================================================================
-- Time32 Builder FFI
-- ============================================================================

@[extern "lean_time32_builder_create"]
opaque time32_builder_create_impl (capacity : USize) (unit : UInt8) : IO (Option Time32BuilderPtr.type)

@[extern "lean_time32_builder_append"]
opaque time32_builder_append_impl (builder : @& Time32BuilderPtr.type) (value : Int32) : IO Bool

@[extern "lean_time32_builder_append_null"]
opaque time32_builder_append_null_impl (builder : @& Time32BuilderPtr.type) : IO Bool

@[extern "lean_time32_builder_finish"]
opaque time32_builder_finish_impl (builder : @& Time32BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_time32_builder_free"]
opaque time32_builder_free_impl (builder : @& Time32BuilderPtr.type) : IO Unit

@[extern "lean_time32_builder_length"]
opaque time32_builder_length_impl (builder : @& Time32BuilderPtr.type) : IO USize

-- ============================================================================
-- Time64 Builder FFI
-- ============================================================================

@[extern "lean_time64_builder_create"]
opaque time64_builder_create_impl (capacity : USize) (unit : UInt8) : IO (Option Time64BuilderPtr.type)

@[extern "lean_time64_builder_append"]
opaque time64_builder_append_impl (builder : @& Time64BuilderPtr.type) (value : Int64) : IO Bool

@[extern "lean_time64_builder_append_null"]
opaque time64_builder_append_null_impl (builder : @& Time64BuilderPtr.type) : IO Bool

@[extern "lean_time64_builder_finish"]
opaque time64_builder_finish_impl (builder : @& Time64BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_time64_builder_free"]
opaque time64_builder_free_impl (builder : @& Time64BuilderPtr.type) : IO Unit

@[extern "lean_time64_builder_length"]
opaque time64_builder_length_impl (builder : @& Time64BuilderPtr.type) : IO USize

-- ============================================================================
-- Duration Builder FFI
-- ============================================================================

@[extern "lean_duration_builder_create"]
opaque duration_builder_create_impl (capacity : USize) (unit : UInt8) : IO (Option DurationBuilderPtr.type)

@[extern "lean_duration_builder_append"]
opaque duration_builder_append_impl (builder : @& DurationBuilderPtr.type) (value : Int64) : IO Bool

@[extern "lean_duration_builder_append_null"]
opaque duration_builder_append_null_impl (builder : @& DurationBuilderPtr.type) : IO Bool

@[extern "lean_duration_builder_finish"]
opaque duration_builder_finish_impl (builder : @& DurationBuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_duration_builder_free"]
opaque duration_builder_free_impl (builder : @& DurationBuilderPtr.type) : IO Unit

@[extern "lean_duration_builder_length"]
opaque duration_builder_length_impl (builder : @& DurationBuilderPtr.type) : IO USize

-- ============================================================================
-- Binary Builder FFI
-- ============================================================================

@[extern "lean_binary_builder_create"]
opaque binary_builder_create_impl (capacity : USize) (dataCapacity : USize) : IO (Option BinaryBuilderPtr.type)

@[extern "lean_binary_builder_append"]
opaque binary_builder_append_impl (builder : @& BinaryBuilderPtr.type) (data : @& ByteArray) : IO Bool

@[extern "lean_binary_builder_append_null"]
opaque binary_builder_append_null_impl (builder : @& BinaryBuilderPtr.type) : IO Bool

@[extern "lean_binary_builder_finish"]
opaque binary_builder_finish_impl (builder : @& BinaryBuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_binary_builder_free"]
opaque binary_builder_free_impl (builder : @& BinaryBuilderPtr.type) : IO Unit

@[extern "lean_binary_builder_length"]
opaque binary_builder_length_impl (builder : @& BinaryBuilderPtr.type) : IO USize

-- ============================================================================
-- Schema Builder FFI
-- ============================================================================

@[extern "lean_schema_builder_create"]
opaque schema_builder_create_impl (capacity : USize) : IO (Option SchemaBuilderPtr.type)

@[extern "lean_schema_builder_add_int64"]
opaque schema_builder_add_int64_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_float64"]
opaque schema_builder_add_float64_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_string"]
opaque schema_builder_add_string_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_timestamp"]
opaque schema_builder_add_timestamp_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (timezone : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_bool"]
opaque schema_builder_add_bool_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_int8"]
opaque schema_builder_add_int8_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_int16"]
opaque schema_builder_add_int16_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_int32"]
opaque schema_builder_add_int32_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_uint8"]
opaque schema_builder_add_uint8_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_uint16"]
opaque schema_builder_add_uint16_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_uint32"]
opaque schema_builder_add_uint32_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_uint64"]
opaque schema_builder_add_uint64_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_float32"]
opaque schema_builder_add_float32_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_date32"]
opaque schema_builder_add_date32_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_date64"]
opaque schema_builder_add_date64_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_time32"]
opaque schema_builder_add_time32_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (unit : UInt8) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_time64"]
opaque schema_builder_add_time64_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (unit : UInt8) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_duration"]
opaque schema_builder_add_duration_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (unit : UInt8) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_add_binary"]
opaque schema_builder_add_binary_impl (builder : @& SchemaBuilderPtr.type) (name : @& String) (nullable : UInt8) : IO Bool

@[extern "lean_schema_builder_finish"]
opaque schema_builder_finish_impl (builder : @& SchemaBuilderPtr.type) : IO (Option ArrowSchemaPtr.type)

@[extern "lean_schema_builder_free"]
opaque schema_builder_free_impl (builder : @& SchemaBuilderPtr.type) : IO Unit

@[extern "lean_schema_builder_field_count"]
opaque schema_builder_field_count_impl (builder : @& SchemaBuilderPtr.type) : IO USize

-- ============================================================================
-- RecordBatch FFI
-- ============================================================================

@[extern "lean_record_batch_create"]
opaque record_batch_create_impl (schema : @& ArrowSchemaPtr.type) (columns : @& Array ArrowArrayPtr.type) (numRows : USize) : IO (Option RecordBatchPtr.type)

@[extern "lean_record_batch_num_rows"]
opaque record_batch_num_rows_impl (batch : @& RecordBatchPtr.type) : IO USize

@[extern "lean_record_batch_num_columns"]
opaque record_batch_num_columns_impl (batch : @& RecordBatchPtr.type) : IO USize

@[extern "lean_record_batch_to_struct_array"]
opaque record_batch_to_struct_array_impl (batch : @& RecordBatchPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_record_batch_free"]
opaque record_batch_free_impl (batch : @& RecordBatchPtr.type) : IO Unit

-- ============================================================================
-- Stream FFI
-- ============================================================================

@[extern "lean_batch_to_stream"]
opaque batch_to_stream_impl (batch : @& RecordBatchPtr.type) : IO (Option ArrowArrayStreamPtr.type)

@[extern "lean_batches_to_stream"]
opaque batches_to_stream_impl (schema : @& ArrowSchemaPtr.type) (batches : @& Array RecordBatchPtr.type) : IO (Option ArrowArrayStreamPtr.type)

-- ============================================================================
-- List Element Type (matches C enum ListElementType)
-- ============================================================================

/-- Element type for list and struct builders -/
inductive ListElementType where
  | int8     : ListElementType
  | int16    : ListElementType
  | int32    : ListElementType
  | int64    : ListElementType
  | uint8    : ListElementType
  | uint16   : ListElementType
  | uint32   : ListElementType
  | uint64   : ListElementType
  | float32  : ListElementType
  | float64  : ListElementType
  | string   : ListElementType
  | bool     : ListElementType
  | binary   : ListElementType
  deriving Inhabited, Repr, BEq

def ListElementType.toUInt8 : ListElementType → UInt8
  | .int8    => 0
  | .int16   => 1
  | .int32   => 2
  | .int64   => 3
  | .uint8   => 4
  | .uint16  => 5
  | .uint32  => 6
  | .uint64  => 7
  | .float32 => 8
  | .float64 => 9
  | .string  => 10
  | .bool    => 11
  | .binary  => 12

-- ============================================================================
-- List Builder FFI
-- ============================================================================

@[extern "lean_list_builder_create"]
opaque list_builder_create_impl (capacity : USize) (elementType : UInt8) : IO (Option ListBuilderPtr.type)

@[extern "lean_list_builder_start_list"]
opaque list_builder_start_list_impl (builder : @& ListBuilderPtr.type) : IO Bool

@[extern "lean_list_builder_finish_list"]
opaque list_builder_finish_list_impl (builder : @& ListBuilderPtr.type) : IO Bool

@[extern "lean_list_builder_append_null"]
opaque list_builder_append_null_impl (builder : @& ListBuilderPtr.type) : IO Bool

@[extern "lean_list_builder_append_int64"]
opaque list_builder_append_int64_impl (builder : @& ListBuilderPtr.type) (value : Int64) : IO Bool

@[extern "lean_list_builder_append_float64"]
opaque list_builder_append_float64_impl (builder : @& ListBuilderPtr.type) (value : Float) : IO Bool

@[extern "lean_list_builder_append_string"]
opaque list_builder_append_string_impl (builder : @& ListBuilderPtr.type) (value : @& String) : IO Bool

@[extern "lean_list_builder_append_bool"]
opaque list_builder_append_bool_impl (builder : @& ListBuilderPtr.type) (value : UInt8) : IO Bool

@[extern "lean_list_builder_finish"]
opaque list_builder_finish_impl (builder : @& ListBuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_list_builder_free"]
opaque list_builder_free_impl (builder : @& ListBuilderPtr.type) : IO Unit

@[extern "lean_list_builder_length"]
opaque list_builder_length_impl (builder : @& ListBuilderPtr.type) : IO USize

-- ============================================================================
-- Struct Builder FFI
-- ============================================================================

@[extern "lean_struct_builder_create"]
opaque struct_builder_create_impl (capacity : USize) : IO (Option StructBuilderPtr.type)

@[extern "lean_struct_builder_add_field"]
opaque struct_builder_add_field_impl (builder : @& StructBuilderPtr.type) (name : @& String) (fieldType : UInt8) : IO Bool

@[extern "lean_struct_builder_append_int64"]
opaque struct_builder_append_int64_impl (builder : @& StructBuilderPtr.type) (fieldIdx : USize) (value : Int64) : IO Bool

@[extern "lean_struct_builder_append_float64"]
opaque struct_builder_append_float64_impl (builder : @& StructBuilderPtr.type) (fieldIdx : USize) (value : Float) : IO Bool

@[extern "lean_struct_builder_append_string"]
opaque struct_builder_append_string_impl (builder : @& StructBuilderPtr.type) (fieldIdx : USize) (value : @& String) : IO Bool

@[extern "lean_struct_builder_append_bool"]
opaque struct_builder_append_bool_impl (builder : @& StructBuilderPtr.type) (fieldIdx : USize) (value : UInt8) : IO Bool

@[extern "lean_struct_builder_append_field_null"]
opaque struct_builder_append_field_null_impl (builder : @& StructBuilderPtr.type) (fieldIdx : USize) : IO Bool

@[extern "lean_struct_builder_append_null"]
opaque struct_builder_append_null_impl (builder : @& StructBuilderPtr.type) : IO Bool

@[extern "lean_struct_builder_finish_row"]
opaque struct_builder_finish_row_impl (builder : @& StructBuilderPtr.type) : IO Bool

@[extern "lean_struct_builder_finish"]
opaque struct_builder_finish_impl (builder : @& StructBuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_struct_builder_get_schema"]
opaque struct_builder_get_schema_impl (builder : @& StructBuilderPtr.type) : IO (Option ArrowSchemaPtr.type)

@[extern "lean_struct_builder_free"]
opaque struct_builder_free_impl (builder : @& StructBuilderPtr.type) : IO Unit

@[extern "lean_struct_builder_length"]
opaque struct_builder_length_impl (builder : @& StructBuilderPtr.type) : IO USize

@[extern "lean_struct_builder_field_count"]
opaque struct_builder_field_count_impl (builder : @& StructBuilderPtr.type) : IO USize

-- ============================================================================
-- Decimal128 Builder FFI
-- ============================================================================

@[extern "lean_decimal128_builder_create"]
opaque decimal128_builder_create_impl (capacity : USize) (precision : Int32) (scale : Int32) : IO (Option Decimal128BuilderPtr.type)

@[extern "lean_decimal128_builder_append"]
opaque decimal128_builder_append_impl (builder : @& Decimal128BuilderPtr.type) (high : Int64) (low : UInt64) : IO Bool

@[extern "lean_decimal128_builder_append_string"]
opaque decimal128_builder_append_string_impl (builder : @& Decimal128BuilderPtr.type) (value : @& String) : IO Bool

@[extern "lean_decimal128_builder_append_null"]
opaque decimal128_builder_append_null_impl (builder : @& Decimal128BuilderPtr.type) : IO Bool

@[extern "lean_decimal128_builder_finish"]
opaque decimal128_builder_finish_impl (builder : @& Decimal128BuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_decimal128_builder_free"]
opaque decimal128_builder_free_impl (builder : @& Decimal128BuilderPtr.type) : IO Unit

@[extern "lean_decimal128_builder_length"]
opaque decimal128_builder_length_impl (builder : @& Decimal128BuilderPtr.type) : IO USize

@[extern "lean_decimal128_builder_precision"]
opaque decimal128_builder_precision_impl (builder : @& Decimal128BuilderPtr.type) : IO Int32

@[extern "lean_decimal128_builder_scale"]
opaque decimal128_builder_scale_impl (builder : @& Decimal128BuilderPtr.type) : IO Int32

-- ============================================================================
-- Dictionary Builder FFI
-- ============================================================================

@[extern "lean_dictionary_builder_create"]
opaque dictionary_builder_create_impl (capacity : USize) (dictCapacity : USize) : IO (Option DictionaryBuilderPtr.type)

@[extern "lean_dictionary_builder_append"]
opaque dictionary_builder_append_impl (builder : @& DictionaryBuilderPtr.type) (value : @& String) : IO Bool

@[extern "lean_dictionary_builder_append_null"]
opaque dictionary_builder_append_null_impl (builder : @& DictionaryBuilderPtr.type) : IO Bool

@[extern "lean_dictionary_builder_finish"]
opaque dictionary_builder_finish_impl (builder : @& DictionaryBuilderPtr.type) : IO (Option ArrowArrayPtr.type)

@[extern "lean_dictionary_builder_free"]
opaque dictionary_builder_free_impl (builder : @& DictionaryBuilderPtr.type) : IO Unit

@[extern "lean_dictionary_builder_length"]
opaque dictionary_builder_length_impl (builder : @& DictionaryBuilderPtr.type) : IO USize

@[extern "lean_dictionary_builder_dict_size"]
opaque dictionary_builder_dict_size_impl (builder : @& DictionaryBuilderPtr.type) : IO USize
