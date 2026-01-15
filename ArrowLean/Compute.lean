/-
  ArrowLean.Compute - Arrow compute functions for array operations

  Provides arithmetic, comparison, aggregation, string operations,
  filter/take/sort, and null handling functions.
-/

import ArrowLean.Ops

namespace ArrowLean.Compute

-- ============================================================================
-- FFI Declarations
-- ============================================================================

-- Arithmetic Operations (element-wise)
@[extern "lean_arrow_add_int64"]
opaque add_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_add_float64"]
opaque add_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_subtract_int64"]
opaque subtract_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_subtract_float64"]
opaque subtract_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_multiply_int64"]
opaque multiply_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_multiply_float64"]
opaque multiply_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_divide_int64"]
opaque divide_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_divide_float64"]
opaque divide_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

-- Scalar arithmetic
@[extern "lean_arrow_add_scalar_int64"]
opaque add_scalar_int64_impl : @& ArrowArrayPtr.type → Int64 → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_add_scalar_float64"]
opaque add_scalar_float64_impl : @& ArrowArrayPtr.type → Float → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_multiply_scalar_int64"]
opaque multiply_scalar_int64_impl : @& ArrowArrayPtr.type → Int64 → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_multiply_scalar_float64"]
opaque multiply_scalar_float64_impl : @& ArrowArrayPtr.type → Float → IO (Option ArrowArrayPtr.type)

-- Unary arithmetic
@[extern "lean_arrow_negate_int64"]
opaque negate_int64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_negate_float64"]
opaque negate_float64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_abs_int64"]
opaque abs_int64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_abs_float64"]
opaque abs_float64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

-- Comparison Operations (return boolean arrays)
@[extern "lean_arrow_eq_int64"]
opaque eq_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_eq_float64"]
opaque eq_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_eq_string"]
opaque eq_string_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_ne_int64"]
opaque ne_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_ne_float64"]
opaque ne_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_lt_int64"]
opaque lt_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_lt_float64"]
opaque lt_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_le_int64"]
opaque le_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_le_float64"]
opaque le_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_gt_int64"]
opaque gt_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_gt_float64"]
opaque gt_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_ge_int64"]
opaque ge_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_ge_float64"]
opaque ge_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

-- Scalar comparisons
@[extern "lean_arrow_eq_scalar_int64"]
opaque eq_scalar_int64_impl : @& ArrowArrayPtr.type → Int64 → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_lt_scalar_int64"]
opaque lt_scalar_int64_impl : @& ArrowArrayPtr.type → Int64 → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_gt_scalar_int64"]
opaque gt_scalar_int64_impl : @& ArrowArrayPtr.type → Int64 → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_eq_scalar_float64"]
opaque eq_scalar_float64_impl : @& ArrowArrayPtr.type → Float → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_lt_scalar_float64"]
opaque lt_scalar_float64_impl : @& ArrowArrayPtr.type → Float → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_gt_scalar_float64"]
opaque gt_scalar_float64_impl : @& ArrowArrayPtr.type → Float → IO (Option ArrowArrayPtr.type)

-- Logical Operations (on boolean arrays)
@[extern "lean_arrow_and"]
opaque and_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_or"]
opaque or_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_not"]
opaque not_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_xor"]
opaque xor_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

-- Aggregation Operations
@[extern "lean_arrow_min_int64"]
opaque min_int64_impl : @& ArrowArrayPtr.type → IO (Option Int)

@[extern "lean_arrow_max_int64"]
opaque max_int64_impl : @& ArrowArrayPtr.type → IO (Option Int)

@[extern "lean_arrow_sum_int64"]
opaque sum_int64_impl : @& ArrowArrayPtr.type → IO (Option Int)

@[extern "lean_arrow_min_float64"]
opaque min_float64_impl : @& ArrowArrayPtr.type → IO (Option Float)

@[extern "lean_arrow_max_float64"]
opaque max_float64_impl : @& ArrowArrayPtr.type → IO (Option Float)

@[extern "lean_arrow_sum_float64"]
opaque sum_float64_impl : @& ArrowArrayPtr.type → IO (Option Float)

@[extern "lean_arrow_mean_int64"]
opaque mean_int64_impl : @& ArrowArrayPtr.type → IO (Option Float)

@[extern "lean_arrow_mean_float64"]
opaque mean_float64_impl : @& ArrowArrayPtr.type → IO (Option Float)

@[extern "lean_arrow_variance_float64"]
opaque variance_float64_impl : @& ArrowArrayPtr.type → IO (Option Float)

@[extern "lean_arrow_stddev_float64"]
opaque stddev_float64_impl : @& ArrowArrayPtr.type → IO (Option Float)

@[extern "lean_arrow_count"]
opaque count_impl : @& ArrowArrayPtr.type → IO Int

@[extern "lean_arrow_count_all"]
opaque count_all_impl : @& ArrowArrayPtr.type → IO Int

@[extern "lean_arrow_count_distinct_int64"]
opaque count_distinct_int64_impl : @& ArrowArrayPtr.type → IO Int

@[extern "lean_arrow_count_distinct_string"]
opaque count_distinct_string_impl : @& ArrowArrayPtr.type → IO Int

@[extern "lean_arrow_any"]
opaque any_impl : @& ArrowArrayPtr.type → IO Bool

@[extern "lean_arrow_all"]
opaque all_impl : @& ArrowArrayPtr.type → IO Bool

-- String Operations
@[extern "lean_arrow_string_length"]
opaque string_length_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_substring"]
opaque substring_impl : @& ArrowArrayPtr.type → Int32 → Int32 → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_string_contains"]
opaque string_contains_impl : @& ArrowArrayPtr.type → @& String → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_string_starts_with"]
opaque string_starts_with_impl : @& ArrowArrayPtr.type → @& String → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_string_ends_with"]
opaque string_ends_with_impl : @& ArrowArrayPtr.type → @& String → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_string_upper"]
opaque string_upper_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_string_lower"]
opaque string_lower_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_string_trim"]
opaque string_trim_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

-- Cast Operations
@[extern "lean_arrow_cast_int64_to_int32"]
opaque cast_int64_to_int32_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_int32_to_int64"]
opaque cast_int32_to_int64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_float64_to_float32"]
opaque cast_float64_to_float32_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_float32_to_float64"]
opaque cast_float32_to_float64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_int64_to_float64"]
opaque cast_int64_to_float64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_int32_to_float64"]
opaque cast_int32_to_float64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_float64_to_int64"]
opaque cast_float64_to_int64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_int64_to_string"]
opaque cast_int64_to_string_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_float64_to_string"]
opaque cast_float64_to_string_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_bool_to_string"]
opaque cast_bool_to_string_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_string_to_int64"]
opaque cast_string_to_int64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_cast_string_to_float64"]
opaque cast_string_to_float64_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

-- Filter/Take/Sort Operations
@[extern "lean_arrow_filter_int64"]
opaque filter_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_filter_float64"]
opaque filter_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_filter_string"]
opaque filter_string_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_filter_bool"]
opaque filter_bool_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_take_int64"]
opaque take_int64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_take_float64"]
opaque take_float64_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_take_string"]
opaque take_string_impl : @& ArrowArrayPtr.type → @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_sort_indices_int64"]
opaque sort_indices_int64_impl : @& ArrowArrayPtr.type → UInt8 → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_sort_indices_float64"]
opaque sort_indices_float64_impl : @& ArrowArrayPtr.type → UInt8 → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_sort_indices_string"]
opaque sort_indices_string_impl : @& ArrowArrayPtr.type → UInt8 → IO (Option ArrowArrayPtr.type)

-- Null Handling
@[extern "lean_arrow_is_null"]
opaque is_null_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_is_valid"]
opaque is_valid_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_fill_null_int64"]
opaque fill_null_int64_impl : @& ArrowArrayPtr.type → Int64 → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_fill_null_float64"]
opaque fill_null_float64_impl : @& ArrowArrayPtr.type → Float → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_fill_null_string"]
opaque fill_null_string_impl : @& ArrowArrayPtr.type → @& String → IO (Option ArrowArrayPtr.type)

@[extern "lean_arrow_drop_null"]
opaque drop_null_impl : @& ArrowArrayPtr.type → IO (Option ArrowArrayPtr.type)

-- ============================================================================
-- High-Level Wrappers
-- ============================================================================

/-- Result of array-returning compute operations -/
abbrev ArrayResult := IO (Option ArrowArray)

/-- Helper to wrap a pointer result as an ArrowArray -/
private def wrapResult (result : Option ArrowArrayPtr.type) : Option ArrowArray :=
  result.map fun ptr => { ptr := ptr, length := 0, null_count := 0, offset := 0 }

-- Arithmetic Operations

/-- Element-wise addition of two Int64 arrays -/
def addInt64 (a b : ArrowArray) : ArrayResult := do
  let result ← add_int64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise addition of two Float64 arrays -/
def addFloat64 (a b : ArrowArray) : ArrayResult := do
  let result ← add_float64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise subtraction of two Int64 arrays -/
def subtractInt64 (a b : ArrowArray) : ArrayResult := do
  let result ← subtract_int64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise subtraction of two Float64 arrays -/
def subtractFloat64 (a b : ArrowArray) : ArrayResult := do
  let result ← subtract_float64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise multiplication of two Int64 arrays -/
def multiplyInt64 (a b : ArrowArray) : ArrayResult := do
  let result ← multiply_int64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise multiplication of two Float64 arrays -/
def multiplyFloat64 (a b : ArrowArray) : ArrayResult := do
  let result ← multiply_float64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise division of two Int64 arrays -/
def divideInt64 (a b : ArrowArray) : ArrayResult := do
  let result ← divide_int64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise division of two Float64 arrays -/
def divideFloat64 (a b : ArrowArray) : ArrayResult := do
  let result ← divide_float64_impl a.ptr b.ptr
  return wrapResult result

/-- Add a scalar to an Int64 array -/
def addScalarInt64 (a : ArrowArray) (scalar : Int64) : ArrayResult := do
  let result ← add_scalar_int64_impl a.ptr scalar
  return wrapResult result

/-- Add a scalar to a Float64 array -/
def addScalarFloat64 (a : ArrowArray) (scalar : Float) : ArrayResult := do
  let result ← add_scalar_float64_impl a.ptr scalar
  return wrapResult result

/-- Multiply an Int64 array by a scalar -/
def multiplyScalarInt64 (a : ArrowArray) (scalar : Int64) : ArrayResult := do
  let result ← multiply_scalar_int64_impl a.ptr scalar
  return wrapResult result

/-- Multiply a Float64 array by a scalar -/
def multiplyScalarFloat64 (a : ArrowArray) (scalar : Float) : ArrayResult := do
  let result ← multiply_scalar_float64_impl a.ptr scalar
  return wrapResult result

/-- Negate an Int64 array -/
def negateInt64 (a : ArrowArray) : ArrayResult := do
  let result ← negate_int64_impl a.ptr
  return wrapResult result

/-- Negate a Float64 array -/
def negateFloat64 (a : ArrowArray) : ArrayResult := do
  let result ← negate_float64_impl a.ptr
  return wrapResult result

/-- Absolute value of an Int64 array -/
def absInt64 (a : ArrowArray) : ArrayResult := do
  let result ← abs_int64_impl a.ptr
  return wrapResult result

/-- Absolute value of a Float64 array -/
def absFloat64 (a : ArrowArray) : ArrayResult := do
  let result ← abs_float64_impl a.ptr
  return wrapResult result

-- Comparison Operations

/-- Element-wise equality comparison of two Int64 arrays -/
def eqInt64 (a b : ArrowArray) : ArrayResult := do
  let result ← eq_int64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise equality comparison of two Float64 arrays -/
def eqFloat64 (a b : ArrowArray) : ArrayResult := do
  let result ← eq_float64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise equality comparison of two string arrays -/
def eqString (a b : ArrowArray) : ArrayResult := do
  let result ← eq_string_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise not-equal comparison of two Int64 arrays -/
def neInt64 (a b : ArrowArray) : ArrayResult := do
  let result ← ne_int64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise not-equal comparison of two Float64 arrays -/
def neFloat64 (a b : ArrowArray) : ArrayResult := do
  let result ← ne_float64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise less-than comparison of two Int64 arrays -/
def ltInt64 (a b : ArrowArray) : ArrayResult := do
  let result ← lt_int64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise less-than comparison of two Float64 arrays -/
def ltFloat64 (a b : ArrowArray) : ArrayResult := do
  let result ← lt_float64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise less-than-or-equal comparison of two Int64 arrays -/
def leInt64 (a b : ArrowArray) : ArrayResult := do
  let result ← le_int64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise less-than-or-equal comparison of two Float64 arrays -/
def leFloat64 (a b : ArrowArray) : ArrayResult := do
  let result ← le_float64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise greater-than comparison of two Int64 arrays -/
def gtInt64 (a b : ArrowArray) : ArrayResult := do
  let result ← gt_int64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise greater-than comparison of two Float64 arrays -/
def gtFloat64 (a b : ArrowArray) : ArrayResult := do
  let result ← gt_float64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise greater-than-or-equal comparison of two Int64 arrays -/
def geInt64 (a b : ArrowArray) : ArrayResult := do
  let result ← ge_int64_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise greater-than-or-equal comparison of two Float64 arrays -/
def geFloat64 (a b : ArrowArray) : ArrayResult := do
  let result ← ge_float64_impl a.ptr b.ptr
  return wrapResult result

/-- Compare Int64 array elements to a scalar for equality -/
def eqScalarInt64 (a : ArrowArray) (scalar : Int64) : ArrayResult := do
  let result ← eq_scalar_int64_impl a.ptr scalar
  return wrapResult result

/-- Compare Int64 array elements to a scalar (less than) -/
def ltScalarInt64 (a : ArrowArray) (scalar : Int64) : ArrayResult := do
  let result ← lt_scalar_int64_impl a.ptr scalar
  return wrapResult result

/-- Compare Int64 array elements to a scalar (greater than) -/
def gtScalarInt64 (a : ArrowArray) (scalar : Int64) : ArrayResult := do
  let result ← gt_scalar_int64_impl a.ptr scalar
  return wrapResult result

/-- Compare Float64 array elements to a scalar for equality -/
def eqScalarFloat64 (a : ArrowArray) (scalar : Float) : ArrayResult := do
  let result ← eq_scalar_float64_impl a.ptr scalar
  return wrapResult result

/-- Compare Float64 array elements to a scalar (less than) -/
def ltScalarFloat64 (a : ArrowArray) (scalar : Float) : ArrayResult := do
  let result ← lt_scalar_float64_impl a.ptr scalar
  return wrapResult result

/-- Compare Float64 array elements to a scalar (greater than) -/
def gtScalarFloat64 (a : ArrowArray) (scalar : Float) : ArrayResult := do
  let result ← gt_scalar_float64_impl a.ptr scalar
  return wrapResult result

-- Logical Operations

/-- Element-wise AND of two boolean arrays -/
def land (a b : ArrowArray) : ArrayResult := do
  let result ← and_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise OR of two boolean arrays -/
def lor (a b : ArrowArray) : ArrayResult := do
  let result ← or_impl a.ptr b.ptr
  return wrapResult result

/-- Element-wise NOT of a boolean array -/
def lnot (a : ArrowArray) : ArrayResult := do
  let result ← not_impl a.ptr
  return wrapResult result

/-- Element-wise XOR of two boolean arrays -/
def lxor (a b : ArrowArray) : ArrayResult := do
  let result ← xor_impl a.ptr b.ptr
  return wrapResult result

-- Aggregation Operations

/-- Minimum value in an Int64 array -/
def minInt64 (a : ArrowArray) : IO (Option Int) :=
  min_int64_impl a.ptr

/-- Maximum value in an Int64 array -/
def maxInt64 (a : ArrowArray) : IO (Option Int) :=
  max_int64_impl a.ptr

/-- Sum of an Int64 array -/
def sumInt64 (a : ArrowArray) : IO (Option Int) :=
  sum_int64_impl a.ptr

/-- Minimum value in a Float64 array -/
def minFloat64 (a : ArrowArray) : IO (Option Float) :=
  min_float64_impl a.ptr

/-- Maximum value in a Float64 array -/
def maxFloat64 (a : ArrowArray) : IO (Option Float) :=
  max_float64_impl a.ptr

/-- Sum of a Float64 array -/
def sumFloat64 (a : ArrowArray) : IO (Option Float) :=
  sum_float64_impl a.ptr

/-- Mean of an Int64 array (returns Float) -/
def meanInt64 (a : ArrowArray) : IO (Option Float) :=
  mean_int64_impl a.ptr

/-- Mean of a Float64 array -/
def meanFloat64 (a : ArrowArray) : IO (Option Float) :=
  mean_float64_impl a.ptr

/-- Variance of a Float64 array -/
def varianceFloat64 (a : ArrowArray) : IO (Option Float) :=
  variance_float64_impl a.ptr

/-- Standard deviation of a Float64 array -/
def stddevFloat64 (a : ArrowArray) : IO (Option Float) :=
  stddev_float64_impl a.ptr

/-- Count non-null values in an array -/
def count (a : ArrowArray) : IO Int :=
  count_impl a.ptr

/-- Count all values in an array (including nulls) -/
def countAll (a : ArrowArray) : IO Int :=
  count_all_impl a.ptr

/-- Count distinct values in an Int64 array -/
def countDistinctInt64 (a : ArrowArray) : IO Int :=
  count_distinct_int64_impl a.ptr

/-- Count distinct values in a string array -/
def countDistinctString (a : ArrowArray) : IO Int :=
  count_distinct_string_impl a.ptr

/-- Check if any value in a boolean array is true -/
def any (a : ArrowArray) : IO Bool :=
  any_impl a.ptr

/-- Check if all values in a boolean array are true -/
def all (a : ArrowArray) : IO Bool :=
  all_impl a.ptr

-- String Operations

/-- Get the length of each string in an array -/
def stringLength (a : ArrowArray) : ArrayResult := do
  let result ← string_length_impl a.ptr
  return wrapResult result

/-- Extract substring from each string in an array -/
def substring (a : ArrowArray) (start : Int32) (len : Int32) : ArrayResult := do
  let result ← substring_impl a.ptr start len
  return wrapResult result

/-- Check if each string contains a pattern -/
def stringContains (a : ArrowArray) (pattern : String) : ArrayResult := do
  let result ← string_contains_impl a.ptr pattern
  return wrapResult result

/-- Check if each string starts with a prefix -/
def stringStartsWith (a : ArrowArray) (pfx : String) : ArrayResult := do
  let result ← string_starts_with_impl a.ptr pfx
  return wrapResult result

/-- Check if each string ends with a suffix -/
def stringEndsWith (a : ArrowArray) (sfx : String) : ArrayResult := do
  let result ← string_ends_with_impl a.ptr sfx
  return wrapResult result

/-- Convert each string to uppercase -/
def stringUpper (a : ArrowArray) : ArrayResult := do
  let result ← string_upper_impl a.ptr
  return wrapResult result

/-- Convert each string to lowercase -/
def stringLower (a : ArrowArray) : ArrayResult := do
  let result ← string_lower_impl a.ptr
  return wrapResult result

/-- Trim whitespace from each string -/
def stringTrim (a : ArrowArray) : ArrayResult := do
  let result ← string_trim_impl a.ptr
  return wrapResult result

-- Cast Operations

/-- Cast Int64 array to Int32 -/
def castInt64ToInt32 (a : ArrowArray) : ArrayResult := do
  let result ← cast_int64_to_int32_impl a.ptr
  return wrapResult result

/-- Cast Int32 array to Int64 -/
def castInt32ToInt64 (a : ArrowArray) : ArrayResult := do
  let result ← cast_int32_to_int64_impl a.ptr
  return wrapResult result

/-- Cast Float64 array to Float32 -/
def castFloat64ToFloat32 (a : ArrowArray) : ArrayResult := do
  let result ← cast_float64_to_float32_impl a.ptr
  return wrapResult result

/-- Cast Float32 array to Float64 -/
def castFloat32ToFloat64 (a : ArrowArray) : ArrayResult := do
  let result ← cast_float32_to_float64_impl a.ptr
  return wrapResult result

/-- Cast Int64 array to Float64 -/
def castInt64ToFloat64 (a : ArrowArray) : ArrayResult := do
  let result ← cast_int64_to_float64_impl a.ptr
  return wrapResult result

/-- Cast Int32 array to Float64 -/
def castInt32ToFloat64 (a : ArrowArray) : ArrayResult := do
  let result ← cast_int32_to_float64_impl a.ptr
  return wrapResult result

/-- Cast Float64 array to Int64 (truncates) -/
def castFloat64ToInt64 (a : ArrowArray) : ArrayResult := do
  let result ← cast_float64_to_int64_impl a.ptr
  return wrapResult result

/-- Cast Int64 array to string -/
def castInt64ToString (a : ArrowArray) : ArrayResult := do
  let result ← cast_int64_to_string_impl a.ptr
  return wrapResult result

/-- Cast Float64 array to string -/
def castFloat64ToString (a : ArrowArray) : ArrayResult := do
  let result ← cast_float64_to_string_impl a.ptr
  return wrapResult result

/-- Cast boolean array to string -/
def castBoolToString (a : ArrowArray) : ArrayResult := do
  let result ← cast_bool_to_string_impl a.ptr
  return wrapResult result

/-- Cast string array to Int64 -/
def castStringToInt64 (a : ArrowArray) : ArrayResult := do
  let result ← cast_string_to_int64_impl a.ptr
  return wrapResult result

/-- Cast string array to Float64 -/
def castStringToFloat64 (a : ArrowArray) : ArrayResult := do
  let result ← cast_string_to_float64_impl a.ptr
  return wrapResult result

-- Filter/Take/Sort Operations

/-- Filter Int64 array by boolean mask -/
def filterInt64 (values mask : ArrowArray) : ArrayResult := do
  let result ← filter_int64_impl values.ptr mask.ptr
  return wrapResult result

/-- Filter Float64 array by boolean mask -/
def filterFloat64 (values mask : ArrowArray) : ArrayResult := do
  let result ← filter_float64_impl values.ptr mask.ptr
  return wrapResult result

/-- Filter string array by boolean mask -/
def filterString (values mask : ArrowArray) : ArrayResult := do
  let result ← filter_string_impl values.ptr mask.ptr
  return wrapResult result

/-- Filter boolean array by boolean mask -/
def filterBool (values mask : ArrowArray) : ArrayResult := do
  let result ← filter_bool_impl values.ptr mask.ptr
  return wrapResult result

/-- Take Int64 values at given indices -/
def takeInt64 (values indices : ArrowArray) : ArrayResult := do
  let result ← take_int64_impl values.ptr indices.ptr
  return wrapResult result

/-- Take Float64 values at given indices -/
def takeFloat64 (values indices : ArrowArray) : ArrayResult := do
  let result ← take_float64_impl values.ptr indices.ptr
  return wrapResult result

/-- Take string values at given indices -/
def takeString (values indices : ArrowArray) : ArrayResult := do
  let result ← take_string_impl values.ptr indices.ptr
  return wrapResult result

/-- Get sort indices for Int64 array -/
def sortIndicesInt64 (values : ArrowArray) (ascending : Bool := true) : ArrayResult := do
  let result ← sort_indices_int64_impl values.ptr (if ascending then 1 else 0)
  return wrapResult result

/-- Get sort indices for Float64 array -/
def sortIndicesFloat64 (values : ArrowArray) (ascending : Bool := true) : ArrayResult := do
  let result ← sort_indices_float64_impl values.ptr (if ascending then 1 else 0)
  return wrapResult result

/-- Get sort indices for string array -/
def sortIndicesString (values : ArrowArray) (ascending : Bool := true) : ArrayResult := do
  let result ← sort_indices_string_impl values.ptr (if ascending then 1 else 0)
  return wrapResult result

-- Null Handling

/-- Check which values are null (returns boolean array) -/
def isNull (a : ArrowArray) : ArrayResult := do
  let result ← is_null_impl a.ptr
  return wrapResult result

/-- Check which values are valid/non-null (returns boolean array) -/
def isValid (a : ArrowArray) : ArrayResult := do
  let result ← is_valid_impl a.ptr
  return wrapResult result

/-- Fill null values in Int64 array with a scalar -/
def fillNullInt64 (a : ArrowArray) (fillValue : Int64) : ArrayResult := do
  let result ← fill_null_int64_impl a.ptr fillValue
  return wrapResult result

/-- Fill null values in Float64 array with a scalar -/
def fillNullFloat64 (a : ArrowArray) (fillValue : Float) : ArrayResult := do
  let result ← fill_null_float64_impl a.ptr fillValue
  return wrapResult result

/-- Fill null values in string array with a scalar -/
def fillNullString (a : ArrowArray) (fillValue : String) : ArrayResult := do
  let result ← fill_null_string_impl a.ptr fillValue
  return wrapResult result

/-- Drop null values from an array -/
def dropNull (a : ArrowArray) : ArrayResult := do
  let result ← drop_null_impl a.ptr
  return wrapResult result

end ArrowLean.Compute
