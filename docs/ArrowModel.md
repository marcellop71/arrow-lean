# ArrowModel - Formal Specification Proposal

A proposal for a formal mathematical model of arrow-lean, inspired by the RedisModel approach.

## Background: The RedisModel Approach

The `redis-lean/RedisModel/AbstractMinimal.lean` demonstrates how to create a formal mathematical specification:

1. **Core Abstraction**: Models Redis as a state monad `RedisM DB α`
2. **Abstract Operations**: A typeclass `AbstractOps` defining `set`, `get`, `del`, `existsKey`
3. **Axiom System**: ~12 axioms capturing operational semantics
4. **Proven Theorems**: Idempotence, commutativity, cancellation, algebraic laws

## What Could ArrowModel Formalize?

Arrow-lean provides columnar data structures (Apache Arrow format). The model could formalize:

---

## 1. Array Builder Model

Formalizes the builder pattern for constructing Arrow arrays.

```lean
namespace ArrowModel.Builder

-- Builder state
structure BuilderState (α : Type) where
  values : Array α
  nullBitmap : Array Bool
  length : Nat

-- Builder monad
abbrev BuilderM (α β : Type) := StateT (BuilderState α) IO β

-- Operations
def append (value : α) : BuilderM α Unit := ...
def appendNull : BuilderM α Unit := ...
def finish : BuilderM α ArrowArray := ...
def length : BuilderM α Nat := ...

-- Axiom: Append increases length by 1
axiom append_increases_length : ∀ (v : α) (state : BuilderState α),
  (≡ append v on state).length = state.length + 1

-- Axiom: AppendNull increases length by 1
axiom append_null_increases_length : ∀ (state : BuilderState α),
  (≡ appendNull on state).length = state.length + 1

-- Axiom: Length matches values array size
axiom length_consistent : ∀ (state : BuilderState α),
  state.length = state.values.size + state.nullBitmap.filter (· = true) |>.size

-- Axiom: Finish produces array with same length
axiom finish_preserves_length : ∀ (state : BuilderState α),
  let arr := (⇐ finish on state)
  arr.length = state.length

-- Axiom: Append preserves order
axiom append_preserves_order : ∀ (v : α) (state : BuilderState α),
  let newState := (≡ append v on state)
  newState.values = state.values.push v
```

---

## 2. Schema Model

Formalizes Arrow schema constraints and field relationships.

```lean
namespace ArrowModel.Schema

-- Arrow data types
inductive ArrowType
  | null | boolean
  | int8 | int16 | int32 | int64
  | uint8 | uint16 | uint32 | uint64
  | float16 | float32 | float64
  | string | binary
  | timestamp (unit : TimeUnit)
  | list (elementType : ArrowType)
  | struct (fields : Array Field)

structure Field where
  name : String
  dtype : ArrowType
  nullable : Bool

structure Schema where
  fields : Array Field

-- Axiom: Field names are unique within schema
axiom field_names_unique : ∀ (schema : Schema),
  schema.fields.map (·.name) |>.toList.Nodup

-- Axiom: Schema is immutable after creation
axiom schema_immutable : ∀ (schema : Schema),
  schema = schema  -- Trivially true, but emphasizes immutability

-- Axiom: Nested types preserve structure
axiom nested_structure : ∀ (schema : Schema) (f : Field),
  f ∈ schema.fields →
  match f.dtype with
  | .list elem => validArrowType elem
  | .struct fields => fields.all (fun sf => validArrowType sf.dtype)
  | _ => True

-- Type compatibility for operations
def compatible (t1 t2 : ArrowType) : Bool :=
  match t1, t2 with
  | .int32, .int64 => true  -- Promotion allowed
  | .float32, .float64 => true
  | _, _ => t1 = t2
```

---

## 3. RecordBatch Model

Formalizes the relationship between schema and data arrays.

```lean
namespace ArrowModel.RecordBatch

structure RecordBatch where
  schema : Schema
  columns : Array ArrowArray
  numRows : Nat

-- Axiom: Number of columns matches schema fields
axiom columns_match_schema : ∀ (batch : RecordBatch),
  batch.columns.size = batch.schema.fields.size

-- Axiom: All columns have same length
axiom columns_same_length : ∀ (batch : RecordBatch),
  batch.columns.all (·.length = batch.numRows)

-- Axiom: Column types match schema field types
axiom column_types_match : ∀ (batch : RecordBatch) (i : Nat),
  i < batch.columns.size →
  batch.columns[i]!.dtype = batch.schema.fields[i]!.dtype

-- Axiom: Concatenation preserves schema
axiom concat_preserves_schema : ∀ (b1 b2 : RecordBatch),
  b1.schema = b2.schema →
  (concat b1 b2).schema = b1.schema

-- Axiom: Concatenation sums rows
axiom concat_sums_rows : ∀ (b1 b2 : RecordBatch),
  b1.schema = b2.schema →
  (concat b1 b2).numRows = b1.numRows + b2.numRows

-- Projection preserves row count
axiom projection_preserves_rows : ∀ (batch : RecordBatch) (indices : Array Nat),
  (project batch indices).numRows = batch.numRows
```

---

## 4. IPC/Serialization Model

Formalizes Arrow IPC format guarantees.

```lean
namespace ArrowModel.IPC

-- Serialization operations
def serialize (batch : RecordBatch) : ByteArray := ...
def deserialize (bytes : ByteArray) : Option RecordBatch := ...

-- Axiom: Roundtrip preserves data
axiom ipc_roundtrip : ∀ (batch : RecordBatch),
  deserialize (serialize batch) = some batch

-- Axiom: Serialization is deterministic
axiom serialize_deterministic : ∀ (batch : RecordBatch),
  serialize batch = serialize batch

-- Axiom: Schema is included in serialization
axiom schema_in_serialization : ∀ (batch : RecordBatch),
  let bytes := serialize batch
  extractSchema bytes = some batch.schema

-- Axiom: Streaming format preserves order
axiom stream_preserves_order : ∀ (batches : Array RecordBatch),
  let stream := serializeStream batches
  deserializeStream stream = batches
```

---

## 5. Parquet Integration Model

Formalizes Parquet file format properties.

```lean
namespace ArrowModel.Parquet

-- Parquet operations
def writeParquet (batch : RecordBatch) (path : String) : IO Unit := ...
def readParquet (path : String) : IO (Option RecordBatch) := ...

-- Axiom: Write then read roundtrip
axiom parquet_roundtrip : ∀ (batch : RecordBatch) (path : String),
  writeParquet batch path *> readParquet path = some batch

-- Axiom: Parquet preserves schema
axiom parquet_preserves_schema : ∀ (batch : RecordBatch) (path : String),
  let readBatch := readParquet path
  readBatch.map (·.schema) = some batch.schema

-- Axiom: Parquet preserves row count
axiom parquet_preserves_rows : ∀ (batch : RecordBatch) (path : String),
  let readBatch := readParquet path
  readBatch.map (·.numRows) = some batch.numRows

-- Column pruning (read only selected columns)
axiom column_pruning : ∀ (path : String) (columns : Array String),
  let batch := readParquetColumns path columns
  batch.schema.fields.map (·.name) = columns
```

---

## 6. Null Handling Model

Formalizes Apache Arrow's null semantics.

```lean
namespace ArrowModel.Null

-- Null propagation in operations
def nullCount (arr : ArrowArray) : Nat := ...
def isNull (arr : ArrowArray) (idx : Nat) : Bool := ...

-- Axiom: Null count is accurate
axiom null_count_accurate : ∀ (arr : ArrowArray),
  nullCount arr = (List.range arr.length).filter (isNull arr) |>.length

-- Axiom: Non-nullable arrays have zero null count
axiom non_nullable_zero_nulls : ∀ (arr : ArrowArray),
  ¬arr.nullable → nullCount arr = 0

-- Axiom: Null propagation in arithmetic
axiom null_propagates : ∀ (arr1 arr2 : ArrowArray) (idx : Nat) (op : Float → Float → Float),
  isNull arr1 idx ∨ isNull arr2 idx →
  isNull (applyOp op arr1 arr2) idx

-- Axiom: Filtering removes nulls if requested
axiom filter_removes_nulls : ∀ (arr : ArrowArray),
  nullCount (dropNull arr) = 0
```

---

## Comparison with RedisModel

| Aspect | RedisModel | ArrowModel |
|--------|------------|------------|
| Core abstraction | Key-value store | Columnar arrays |
| State type | DB (opaque) | Schema + Arrays |
| Operations | GET, SET, DEL | append, finish, concat |
| Key invariants | set-get consistency | schema/data alignment |
| Composition | monadic DB ops | batch concatenation |

---

## Recommended Implementation Order

1. **Builder Model** - Foundational, tests append semantics
2. **Schema Model** - Type system for arrays
3. **RecordBatch Model** - Combines schema + data
4. **Null Handling** - Cross-cutting concern
5. **IPC/Parquet** - Serialization guarantees

## Why Model Arrow Formally?

1. **Type Safety**: Ensure column types match schema at compile time
2. **Data Integrity**: Verify null handling and length consistency
3. **Serialization Correctness**: Prove IPC/Parquet roundtrips preserve data
4. **Schema Evolution**: Formalize compatible schema changes
