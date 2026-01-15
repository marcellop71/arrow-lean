#ifndef ARROW_COMPUTE_H
#define ARROW_COMPUTE_H

#include "arrow_c_abi.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Aggregation Result
// ============================================================================

typedef struct {
    bool is_valid;
    int64_t i64_value;
    double f64_value;
} AggregateResult;

// ============================================================================
// Arithmetic Operations
// ============================================================================

// Element-wise addition
struct ArrowArray* arrow_add_int64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_add_float64(struct ArrowArray* a, struct ArrowArray* b);

// Element-wise subtraction
struct ArrowArray* arrow_subtract_int64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_subtract_float64(struct ArrowArray* a, struct ArrowArray* b);

// Element-wise multiplication
struct ArrowArray* arrow_multiply_int64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_multiply_float64(struct ArrowArray* a, struct ArrowArray* b);

// Element-wise division
struct ArrowArray* arrow_divide_int64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_divide_float64(struct ArrowArray* a, struct ArrowArray* b);

// Scalar arithmetic
struct ArrowArray* arrow_add_scalar_int64(struct ArrowArray* a, int64_t scalar);
struct ArrowArray* arrow_add_scalar_float64(struct ArrowArray* a, double scalar);
struct ArrowArray* arrow_multiply_scalar_int64(struct ArrowArray* a, int64_t scalar);
struct ArrowArray* arrow_multiply_scalar_float64(struct ArrowArray* a, double scalar);

// Negation
struct ArrowArray* arrow_negate_int64(struct ArrowArray* a);
struct ArrowArray* arrow_negate_float64(struct ArrowArray* a);

// Absolute value
struct ArrowArray* arrow_abs_int64(struct ArrowArray* a);
struct ArrowArray* arrow_abs_float64(struct ArrowArray* a);

// ============================================================================
// Comparison Operations (return boolean arrays)
// ============================================================================

// Equal
struct ArrowArray* arrow_eq_int64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_eq_float64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_eq_string(struct ArrowArray* a, struct ArrowArray* b);

// Not equal
struct ArrowArray* arrow_ne_int64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_ne_float64(struct ArrowArray* a, struct ArrowArray* b);

// Less than
struct ArrowArray* arrow_lt_int64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_lt_float64(struct ArrowArray* a, struct ArrowArray* b);

// Less than or equal
struct ArrowArray* arrow_le_int64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_le_float64(struct ArrowArray* a, struct ArrowArray* b);

// Greater than
struct ArrowArray* arrow_gt_int64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_gt_float64(struct ArrowArray* a, struct ArrowArray* b);

// Greater than or equal
struct ArrowArray* arrow_ge_int64(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_ge_float64(struct ArrowArray* a, struct ArrowArray* b);

// Scalar comparisons
struct ArrowArray* arrow_eq_scalar_int64(struct ArrowArray* a, int64_t scalar);
struct ArrowArray* arrow_lt_scalar_int64(struct ArrowArray* a, int64_t scalar);
struct ArrowArray* arrow_gt_scalar_int64(struct ArrowArray* a, int64_t scalar);
struct ArrowArray* arrow_eq_scalar_float64(struct ArrowArray* a, double scalar);
struct ArrowArray* arrow_lt_scalar_float64(struct ArrowArray* a, double scalar);
struct ArrowArray* arrow_gt_scalar_float64(struct ArrowArray* a, double scalar);

// ============================================================================
// Logical Operations (on boolean arrays)
// ============================================================================

struct ArrowArray* arrow_and(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_or(struct ArrowArray* a, struct ArrowArray* b);
struct ArrowArray* arrow_not(struct ArrowArray* a);
struct ArrowArray* arrow_xor(struct ArrowArray* a, struct ArrowArray* b);

// ============================================================================
// Aggregation Operations
// ============================================================================

// Min/Max
AggregateResult arrow_min_int64(struct ArrowArray* a);
AggregateResult arrow_max_int64(struct ArrowArray* a);
AggregateResult arrow_min_float64(struct ArrowArray* a);
AggregateResult arrow_max_float64(struct ArrowArray* a);

// Sum
AggregateResult arrow_sum_int64(struct ArrowArray* a);
AggregateResult arrow_sum_float64(struct ArrowArray* a);

// Mean (always returns float64)
AggregateResult arrow_mean_int64(struct ArrowArray* a);
AggregateResult arrow_mean_float64(struct ArrowArray* a);

// Variance and standard deviation
AggregateResult arrow_variance_float64(struct ArrowArray* a);
AggregateResult arrow_stddev_float64(struct ArrowArray* a);

// Count (non-null values)
int64_t arrow_count(struct ArrowArray* a);

// Count all (including nulls)
int64_t arrow_count_all(struct ArrowArray* a);

// Count distinct values
int64_t arrow_count_distinct_int64(struct ArrowArray* a);
int64_t arrow_count_distinct_string(struct ArrowArray* a);

// Any/All for boolean arrays
bool arrow_any(struct ArrowArray* a);
bool arrow_all(struct ArrowArray* a);

// ============================================================================
// String Operations
// ============================================================================

// String length (returns int32 array)
struct ArrowArray* arrow_string_length(struct ArrowArray* strings);

// Substring extraction
struct ArrowArray* arrow_substring(struct ArrowArray* strings, int32_t start, int32_t length);

// Contains pattern (returns boolean array)
struct ArrowArray* arrow_string_contains(struct ArrowArray* strings, const char* pattern);

// Starts with prefix (returns boolean array)
struct ArrowArray* arrow_string_starts_with(struct ArrowArray* strings, const char* prefix);

// Ends with suffix (returns boolean array)
struct ArrowArray* arrow_string_ends_with(struct ArrowArray* strings, const char* suffix);

// Upper/Lower case
struct ArrowArray* arrow_string_upper(struct ArrowArray* strings);
struct ArrowArray* arrow_string_lower(struct ArrowArray* strings);

// Trim whitespace
struct ArrowArray* arrow_string_trim(struct ArrowArray* strings);

// ============================================================================
// Cast Operations
// ============================================================================

// Integer casts
struct ArrowArray* arrow_cast_int64_to_int32(struct ArrowArray* a);
struct ArrowArray* arrow_cast_int32_to_int64(struct ArrowArray* a);

// Float casts
struct ArrowArray* arrow_cast_float64_to_float32(struct ArrowArray* a);
struct ArrowArray* arrow_cast_float32_to_float64(struct ArrowArray* a);

// Int to float
struct ArrowArray* arrow_cast_int64_to_float64(struct ArrowArray* a);
struct ArrowArray* arrow_cast_int32_to_float64(struct ArrowArray* a);

// Float to int (truncates)
struct ArrowArray* arrow_cast_float64_to_int64(struct ArrowArray* a);

// To string
struct ArrowArray* arrow_cast_int64_to_string(struct ArrowArray* a);
struct ArrowArray* arrow_cast_float64_to_string(struct ArrowArray* a);
struct ArrowArray* arrow_cast_bool_to_string(struct ArrowArray* a);

// From string
struct ArrowArray* arrow_cast_string_to_int64(struct ArrowArray* a);
struct ArrowArray* arrow_cast_string_to_float64(struct ArrowArray* a);

// ============================================================================
// Filter/Take/Sort Operations
// ============================================================================

// Filter: select elements where mask is true
struct ArrowArray* arrow_filter_int64(struct ArrowArray* values, struct ArrowArray* bool_mask);
struct ArrowArray* arrow_filter_float64(struct ArrowArray* values, struct ArrowArray* bool_mask);
struct ArrowArray* arrow_filter_string(struct ArrowArray* values, struct ArrowArray* bool_mask);
struct ArrowArray* arrow_filter_bool(struct ArrowArray* values, struct ArrowArray* bool_mask);

// Take: select elements by index
struct ArrowArray* arrow_take_int64(struct ArrowArray* values, struct ArrowArray* int32_indices);
struct ArrowArray* arrow_take_float64(struct ArrowArray* values, struct ArrowArray* int32_indices);
struct ArrowArray* arrow_take_string(struct ArrowArray* values, struct ArrowArray* int32_indices);

// Sort indices (returns int32 array of indices)
struct ArrowArray* arrow_sort_indices_int64(struct ArrowArray* values, bool ascending);
struct ArrowArray* arrow_sort_indices_float64(struct ArrowArray* values, bool ascending);
struct ArrowArray* arrow_sort_indices_string(struct ArrowArray* values, bool ascending);

// ============================================================================
// Null Handling
// ============================================================================

// Check if value is null (returns boolean array)
struct ArrowArray* arrow_is_null(struct ArrowArray* a);

// Check if value is not null (returns boolean array)
struct ArrowArray* arrow_is_valid(struct ArrowArray* a);

// Fill null values with a scalar
struct ArrowArray* arrow_fill_null_int64(struct ArrowArray* a, int64_t fill_value);
struct ArrowArray* arrow_fill_null_float64(struct ArrowArray* a, double fill_value);
struct ArrowArray* arrow_fill_null_string(struct ArrowArray* a, const char* fill_value);

// Drop null values (returns filtered array)
struct ArrowArray* arrow_drop_null(struct ArrowArray* a);

// ============================================================================
// Utility Functions
// ============================================================================

// Free an array created by compute functions
void arrow_compute_array_free(struct ArrowArray* array);

// Get the format string for an array (for debugging)
const char* arrow_compute_get_format(struct ArrowArray* array);

#ifdef __cplusplus
}
#endif

#endif // ARROW_COMPUTE_H
