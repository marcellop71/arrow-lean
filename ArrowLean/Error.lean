/-
  ArrowLean/Error.lean - Typed error handling for Arrow operations
-/

namespace Arrow

/-- Categories of Arrow errors -/
inductive ErrorKind
  | nullAccess        -- Attempted to read a null value
  | typeMismatch      -- Column type doesn't match requested type
  | indexOutOfBounds  -- Array index out of range
  | columnNotFound    -- Named column doesn't exist
  | invalidSchema     -- Schema is malformed or incompatible
  | allocationFailed  -- Memory allocation failed
  | serializationFailed -- IPC or Parquet serialization error
  | ioError           -- General I/O error
  | other             -- Catch-all for other errors
  deriving Repr, BEq

/-- Arrow error with kind and message -/
structure Error where
  kind : ErrorKind
  message : String
  deriving Repr

instance : ToString Error where
  toString e := s!"Arrow.Error.{repr e.kind}: {e.message}"

namespace Error

def nullAccess (msg : String := "Attempted to access null value") : Error :=
  { kind := .nullAccess, message := msg }

def typeMismatch (expected actual : String) : Error :=
  { kind := .typeMismatch, message := s!"Type mismatch: expected {expected}, got {actual}" }

def indexOutOfBounds (index length : Nat) : Error :=
  { kind := .indexOutOfBounds, message := s!"Index {index} out of bounds (length {length})" }

def columnNotFound (name : String) : Error :=
  { kind := .columnNotFound, message := s!"Column '{name}' not found" }

def invalidSchema (msg : String) : Error :=
  { kind := .invalidSchema, message := msg }

def allocationFailed (msg : String := "Memory allocation failed") : Error :=
  { kind := .allocationFailed, message := msg }

def serializationFailed (msg : String) : Error :=
  { kind := .serializationFailed, message := msg }

def ioError (msg : String) : Error :=
  { kind := .ioError, message := msg }

def other (msg : String) : Error :=
  { kind := .other, message := msg }

/-- Convert IO.Error to Arrow.Error -/
def fromIOError (e : IO.Error) : Error :=
  { kind := .ioError, message := toString e }

end Error

/-- Result type for Arrow operations -/
abbrev Result (α : Type) := Except Error α

end Arrow
