/-
  ArrowLean/Monad.lean - Monadic interface for Arrow table operations

  Provides ArrowM monad for chaining operations on Arrow tables with
  typed error handling and automatic resource management.
-/

import ArrowLean.Ops
import ArrowLean.FFI
import ArrowLean.Utils
import ArrowLean.Error
import Std.Data.HashMap

namespace Arrow

/-- Configuration for Arrow operations -/
structure Config where
  /-- Whether to check bounds on array access -/
  boundsCheck : Bool := true
  deriving Repr

/-- Table context: schema paired with array data -/
structure TableContext where
  schema : ArrowSchema
  array : ArrowArray
  /-- Column name to index mapping (cached for performance) -/
  columnIndex : Std.HashMap String Nat := {}

/-- The Arrow monad: ReaderT for table context, ExceptT for errors, IO for effects -/
abbrev ArrowM := ReaderT TableContext $ ExceptT Error IO

/-- Lift an IO action into ArrowM -/
def liftIO (action : IO α) : ArrowM α := fun _ => ExceptT.lift action

/-- Lift an IO action that might fail, converting IO.Error to Arrow.Error -/
def liftIOExcept (action : IO α) (toError : IO.Error → Error) : ArrowM α := fun _ => do
  match ← action.toBaseIO with
  | .ok a => return a
  | .error e => throw (toError e)

/-- Get the current table context -/
def getContext : ArrowM TableContext := read

/-- Get the schema from current context -/
def getSchema : ArrowM ArrowSchema := do
  let ctx ← read
  return ctx.schema

/-- Get the array from current context -/
def getArray : ArrowM ArrowArray := do
  let ctx ← read
  return ctx.array

/-- Get the number of rows in the table -/
def rowCount : ArrowM Nat := do
  let ctx ← read
  return ctx.array.length.toNat

/-- Run an ArrowM computation with a table context -/
def runArrowM (ctx : TableContext) (comp : ArrowM α) : IO (Except Error α) :=
  ExceptT.run (comp.run ctx)

/-- Run an ArrowM computation with schema and array -/
def withTable (schema : ArrowSchema) (array : ArrowArray) (comp : ArrowM α) : IO (Except Error α) :=
  runArrowM { schema, array } comp

/-- Run with automatic resource cleanup -/
def withTableBracket
    (schema : ArrowSchema)
    (array : ArrowArray)
    (comp : ArrowM α) : IO (Except Error α) := do
  try
    runArrowM { schema, array } comp
  finally
    schema.release
    array.release

/-! ## Column Access -/

/-- Get a column value as Int64 at the given row index -/
def getInt64At (row : Nat) : ArrowM (Option Int64) := do
  let ctx ← read
  let len := ctx.array.length.toNat
  if row >= len then
    throw (Error.indexOutOfBounds row len)
  liftIOExcept (ctx.array.getInt64 row.toUSize) Error.fromIOError

/-- Get a column value as UInt64 at the given row index -/
def getUInt64At (row : Nat) : ArrowM (Option UInt64) := do
  let ctx ← read
  let len := ctx.array.length.toNat
  if row >= len then
    throw (Error.indexOutOfBounds row len)
  liftIOExcept (ctx.array.getUInt64 row.toUSize) Error.fromIOError

/-- Get a column value as Float64 at the given row index -/
def getFloat64At (row : Nat) : ArrowM (Option Float) := do
  let ctx ← read
  let len := ctx.array.length.toNat
  if row >= len then
    throw (Error.indexOutOfBounds row len)
  liftIOExcept (ctx.array.getFloat64 row.toUSize) Error.fromIOError

/-- Get a column value as String at the given row index -/
def getStringAt (row : Nat) : ArrowM (Option String) := do
  let ctx ← read
  let len := ctx.array.length.toNat
  if row >= len then
    throw (Error.indexOutOfBounds row len)
  liftIOExcept (ctx.array.getString row.toUSize) Error.fromIOError

/-- Get a column value as Bool at the given row index -/
def getBoolAt (row : Nat) : ArrowM (Option Bool) := do
  let ctx ← read
  let len := ctx.array.length.toNat
  if row >= len then
    throw (Error.indexOutOfBounds row len)
  liftIOExcept (ctx.array.getBool row.toUSize) Error.fromIOError

/-! ## Iteration -/

/-- Iterate over all rows, collecting results -/
def mapRows (f : Nat → ArrowM α) : ArrowM (Array α) := do
  let n ← rowCount
  let mut results := #[]
  for i in [:n] do
    let v ← f i
    results := results.push v
  return results

/-- Iterate over all rows with side effects -/
def forEachRow (f : Nat → ArrowM Unit) : ArrowM Unit := do
  let n ← rowCount
  for i in [:n] do
    f i

/-- Fold over all rows -/
def foldRows (init : β) (f : β → Nat → ArrowM β) : ArrowM β := do
  let n ← rowCount
  let mut acc := init
  for i in [:n] do
    acc ← f acc i
  return acc

/-! ## Filtering -/

/-- Collect row indices that satisfy a predicate -/
def filterRowIndices (pred : Nat → ArrowM Bool) : ArrowM (Array Nat) := do
  let n ← rowCount
  let mut indices := #[]
  for i in [:n] do
    if ← pred i then
      indices := indices.push i
  return indices

/-! ## Aggregation helpers -/

/-- Sum all non-null Int64 values -/
def sumInt64 : ArrowM Int64 := do
  foldRows 0 fun acc i => do
    match ← getInt64At i with
    | some v => return acc + v
    | none => return acc

/-- Sum all non-null Float64 values -/
def sumFloat64 : ArrowM Float := do
  foldRows 0.0 fun acc i => do
    match ← getFloat64At i with
    | some v => return acc + v
    | none => return acc

/-- Count non-null values -/
def countNonNull (getValue : Nat → ArrowM (Option α)) : ArrowM Nat := do
  foldRows 0 fun acc i => do
    match ← getValue i with
    | some _ => return acc + 1
    | none => return acc

/-- Average of non-null Float64 values -/
def avgFloat64 : ArrowM (Option Float) := do
  let (sum, count) ← foldRows (0.0, 0) fun (s, c) i => do
    match ← getFloat64At i with
    | some v => return (s + v, c + 1)
    | none => return (s, c)
  if count == 0 then
    return none
  else
    return some (sum / Float.ofNat count)

end Arrow

-- ============================================================================
-- ArrowIO: Simpler monad for Arrow operations without table context
-- ============================================================================

namespace ArrowLean

/-- ArrowIO monad: ExceptT with Arrow.Error over IO -/
abbrev ArrowIO := ExceptT Arrow.Error IO

namespace ArrowIO

/-- Lift an IO action into ArrowIO -/
def liftIO (action : IO α) : ArrowIO α := ExceptT.lift action

/-- Lift an Option to ArrowIO, using the given error if None -/
def fromOption (opt : Option α) (error : Arrow.Error) : ArrowIO α :=
  match opt with
  | some a => pure a
  | none => throw error

/-- Lift an Option from IO to ArrowIO -/
def fromOptionIO (action : IO (Option α)) (error : Arrow.Error) : ArrowIO α := do
  match ← ExceptT.lift action with
  | some a => pure a
  | none => throw error

/-- Run an ArrowIO computation -/
def run (comp : ArrowIO α) : IO (Except Arrow.Error α) := ExceptT.run comp

/-- Run an ArrowIO computation, returning a default value on error -/
def runOrDefault (comp : ArrowIO α) (default : α) : IO α := do
  match ← comp.run with
  | .ok a => return a
  | .error _ => return default

/-- Run an ArrowIO computation, panicking on error -/
def runOrPanic [Inhabited α] (comp : ArrowIO α) : IO α := do
  match ← comp.run with
  | .ok a => return a
  | .error e => panic! s!"Arrow error: {e}"

/-- Map error type -/
def mapError (f : Arrow.Error → Arrow.Error) (comp : ArrowIO α) : ArrowIO α :=
  comp.tryCatch fun e => throw (f e)

/-- Recover from error with a default value -/
def recover (comp : ArrowIO α) (default : α) : ArrowIO α :=
  comp.tryCatch fun _ => pure default

/-- Recover from error with a computation -/
def recoverWith (comp : ArrowIO α) (recovery : Arrow.Error → ArrowIO α) : ArrowIO α :=
  comp.tryCatch recovery

end ArrowIO

-- ============================================================================
-- Bracket patterns for resource management
-- ============================================================================

/-- Bracket pattern: acquire, use, release -/
def bracket (acquire : ArrowIO α) (release : α → IO Unit) (use : α → ArrowIO β) : ArrowIO β := do
  let resource ← acquire
  try
    use resource
  finally
    ArrowIO.liftIO (release resource)

/-- Bracket for ArrowSchema -/
def withSchema (format : String) (action : ArrowSchema → ArrowIO α) : ArrowIO α := do
  let schema ← ArrowIO.liftIO (ArrowSchema.init format)
  try
    action schema
  finally
    ArrowIO.liftIO schema.release

/-- Bracket for ArrowArray -/
def withArray (length : UInt64) (action : ArrowArray → ArrowIO α) : ArrowIO α := do
  let array ← ArrowIO.liftIO (ArrowArray.init length)
  try
    action array
  finally
    ArrowIO.liftIO array.release

/-- Bracket for schema and array together -/
def withSchemaAndArray (format : String) (length : UInt64)
    (action : ArrowSchema → ArrowArray → ArrowIO α) : ArrowIO α := do
  let schema ← ArrowIO.liftIO (ArrowSchema.init format)
  let array ← ArrowIO.liftIO (ArrowArray.init length)
  try
    action schema array
  finally
    ArrowIO.liftIO schema.release
    ArrowIO.liftIO array.release

-- ============================================================================
-- Conversion utilities
-- ============================================================================

/-- Convert Option to ArrowIO with a specific error -/
def Option.toArrowIO (opt : Option α) (error : Arrow.Error) : ArrowIO α :=
  ArrowIO.fromOption opt error

/-- Convert Option to ArrowIO with allocation failed error -/
def Option.toArrowIOAlloc (opt : Option α) (msg : String := "Allocation failed") : ArrowIO α :=
  ArrowIO.fromOption opt (Arrow.Error.allocationFailed msg)

/-- Convert Option to ArrowIO with null access error -/
def Option.toArrowIONull (opt : Option α) (msg : String := "Null value") : ArrowIO α :=
  ArrowIO.fromOption opt (Arrow.Error.nullAccess msg)

/-- Sequence an array of ArrowIO computations -/
def ArrowIO.sequence (actions : Array (ArrowIO α)) : ArrowIO (Array α) := do
  let mut results := #[]
  for action in actions do
    results := results.push (← action)
  return results

/-- Try each action until one succeeds -/
def ArrowIO.firstSuccess (actions : Array (ArrowIO α)) : ArrowIO α := do
  let mut lastError : Arrow.Error := Arrow.Error.other "No actions provided"
  for action in actions do
    match ← ExceptT.lift (action.run) with
    | .ok a => return a
    | .error e => lastError := e
  throw lastError

/-- Run actions in parallel (simplified - actually runs sequentially) -/
def ArrowIO.parallel (actions : Array (ArrowIO α)) : ArrowIO (Array α) :=
  ArrowIO.sequence actions

end ArrowLean
