-- Core Lean type candidates for Arrow data structures

-- Opaque pointer type for C structures
opaque ArrowSchemaPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque ArrowArrayPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque ArrowArrayStreamPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩
opaque ArrowBufferPtr : NonemptyType := ⟨Unit, ⟨()⟩⟩

-- Provide explicit Nonempty instances for the opaque types
instance : Nonempty ArrowSchemaPtr.type := ArrowSchemaPtr.property
instance : Nonempty ArrowArrayPtr.type := ArrowArrayPtr.property
instance : Nonempty ArrowArrayStreamPtr.type := ArrowArrayStreamPtr.property
instance : Nonempty ArrowBufferPtr.type := ArrowBufferPtr.property

-- Lean-side Arrow objects that wrap C pointers
structure ArrowSchema where
  ptr : ArrowSchemaPtr.type
  -- Cached Lean data (optional for performance)
  format : String
  name : Option String
  flags : UInt64
  deriving Nonempty

structure ArrowArray where
  ptr : ArrowArrayPtr.type
  -- Cached Lean data (optional for performance)
  length : UInt64
  null_count : UInt64
  offset : UInt64
  deriving Nonempty

structure ArrowArrayStream where
  ptr : ArrowArrayStreamPtr.type
  deriving Nonempty

-- Time units for timestamp types
inductive TimeUnit : Type
| second
| millisecond
| microsecond
| nanosecond
deriving Nonempty

-- Arrow data types (fixed universe to avoid type mismatch with TimeUnit)
inductive ArrowType : Type
| null
| boolean
| int8 | int16 | int32 | int64
| uint8 | uint16 | uint32 | uint64
| float16 | float32 | float64
| string | binary
| timestamp (unit: TimeUnit)
| date32 | date64
| list (element_type: ArrowType)
| struct (fields: Array (String × ArrowType))
deriving Nonempty

-- Memory management
structure ArrowBuffer where
  ptr : ArrowBufferPtr.type
  size : USize
  capacity : USize
  deriving Nonempty
