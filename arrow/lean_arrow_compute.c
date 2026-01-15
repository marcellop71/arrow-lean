/**
 * lean_arrow_compute.c - Lean FFI wrappers for Arrow compute functions
 *
 * Provides Lean-compatible interfaces to arithmetic, comparison,
 * aggregation, string, and filter/take/sort operations.
 */

#include <lean/lean.h>
#include "arrow_compute.h"
#include "arrow_wrapper.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Helper functions for Option type
// ============================================================================

static lean_obj_res lean_mk_option_none(void) {
    return lean_alloc_ctor(0, 0, 0);
}

static lean_obj_res lean_mk_option_some(lean_obj_arg value) {
    lean_object* option = lean_alloc_ctor(1, 1, 0);
    lean_ctor_set(option, 0, value);
    return option;
}

// ============================================================================
// Helper: Wrap ArrowArray result for Lean
// ============================================================================

static inline lean_obj_res wrap_array_result(struct ArrowArray* array) {
    if (!array) {
        return lean_mk_option_none();
    }
    // Box the pointer as usize for Lean
    return lean_mk_option_some(lean_box_usize((uintptr_t)array));
}

// ============================================================================
// Arithmetic Operations
// ============================================================================

LEAN_EXPORT lean_obj_res lean_arrow_add_int64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_add_int64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_add_float64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_add_float64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_subtract_int64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_subtract_int64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_subtract_float64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_subtract_float64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_multiply_int64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_multiply_int64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_multiply_float64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_multiply_float64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_divide_int64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_divide_int64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_divide_float64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_divide_float64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

// Scalar arithmetic
LEAN_EXPORT lean_obj_res lean_arrow_add_scalar_int64(b_lean_obj_arg a_ptr, int64_t scalar, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_add_scalar_int64(a, scalar);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_add_scalar_float64(b_lean_obj_arg a_ptr, double scalar, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_add_scalar_float64(a, scalar);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_multiply_scalar_int64(b_lean_obj_arg a_ptr, int64_t scalar, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_multiply_scalar_int64(a, scalar);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_multiply_scalar_float64(b_lean_obj_arg a_ptr, double scalar, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_multiply_scalar_float64(a, scalar);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

// Unary arithmetic
LEAN_EXPORT lean_obj_res lean_arrow_negate_int64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_negate_int64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_negate_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_negate_float64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_abs_int64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_abs_int64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_abs_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_abs_float64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

// ============================================================================
// Comparison Operations
// ============================================================================

LEAN_EXPORT lean_obj_res lean_arrow_eq_int64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_eq_int64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_eq_float64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_eq_float64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_eq_string(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_eq_string(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_ne_int64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_ne_int64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_ne_float64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_ne_float64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_lt_int64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_lt_int64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_lt_float64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_lt_float64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_le_int64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_le_int64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_le_float64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_le_float64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_gt_int64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_gt_int64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_gt_float64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_gt_float64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_ge_int64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_ge_int64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_ge_float64(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_ge_float64(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

// Scalar comparisons
LEAN_EXPORT lean_obj_res lean_arrow_eq_scalar_int64(b_lean_obj_arg a_ptr, int64_t scalar, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_eq_scalar_int64(a, scalar);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_lt_scalar_int64(b_lean_obj_arg a_ptr, int64_t scalar, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_lt_scalar_int64(a, scalar);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_gt_scalar_int64(b_lean_obj_arg a_ptr, int64_t scalar, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_gt_scalar_int64(a, scalar);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_eq_scalar_float64(b_lean_obj_arg a_ptr, double scalar, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_eq_scalar_float64(a, scalar);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_lt_scalar_float64(b_lean_obj_arg a_ptr, double scalar, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_lt_scalar_float64(a, scalar);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_gt_scalar_float64(b_lean_obj_arg a_ptr, double scalar, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_gt_scalar_float64(a, scalar);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

// ============================================================================
// Logical Operations
// ============================================================================

LEAN_EXPORT lean_obj_res lean_arrow_and(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_and(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_or(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_or(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_not(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_not(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_xor(b_lean_obj_arg a_ptr, b_lean_obj_arg b_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* b = (struct ArrowArray*)lean_get_external_data(b_ptr);
    struct ArrowArray* result = arrow_xor(a, b);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

// ============================================================================
// Aggregation Operations
// ============================================================================

// Returns (is_valid, value) as a pair
LEAN_EXPORT lean_obj_res lean_arrow_min_int64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    AggregateResult result = arrow_min_int64(a);
    if (!result.is_valid) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_int64_to_int(result.i64_value)));
}

LEAN_EXPORT lean_obj_res lean_arrow_max_int64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    AggregateResult result = arrow_max_int64(a);
    if (!result.is_valid) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_int64_to_int(result.i64_value)));
}

LEAN_EXPORT lean_obj_res lean_arrow_sum_int64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    AggregateResult result = arrow_sum_int64(a);
    if (!result.is_valid) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_int64_to_int(result.i64_value)));
}

LEAN_EXPORT lean_obj_res lean_arrow_min_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    AggregateResult result = arrow_min_float64(a);
    if (!result.is_valid) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_float(result.f64_value)));
}

LEAN_EXPORT lean_obj_res lean_arrow_max_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    AggregateResult result = arrow_max_float64(a);
    if (!result.is_valid) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_float(result.f64_value)));
}

LEAN_EXPORT lean_obj_res lean_arrow_sum_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    AggregateResult result = arrow_sum_float64(a);
    if (!result.is_valid) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_float(result.f64_value)));
}

LEAN_EXPORT lean_obj_res lean_arrow_mean_int64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    AggregateResult result = arrow_mean_int64(a);
    if (!result.is_valid) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_float(result.f64_value)));
}

LEAN_EXPORT lean_obj_res lean_arrow_mean_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    AggregateResult result = arrow_mean_float64(a);
    if (!result.is_valid) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_float(result.f64_value)));
}

LEAN_EXPORT lean_obj_res lean_arrow_variance_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    AggregateResult result = arrow_variance_float64(a);
    if (!result.is_valid) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_float(result.f64_value)));
}

LEAN_EXPORT lean_obj_res lean_arrow_stddev_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    AggregateResult result = arrow_stddev_float64(a);
    if (!result.is_valid) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_float(result.f64_value)));
}

LEAN_EXPORT lean_obj_res lean_arrow_count(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    int64_t result = arrow_count(a);
    return lean_io_result_mk_ok(lean_int64_to_int(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_count_all(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    int64_t result = arrow_count_all(a);
    return lean_io_result_mk_ok(lean_int64_to_int(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_count_distinct_int64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    int64_t result = arrow_count_distinct_int64(a);
    return lean_io_result_mk_ok(lean_int64_to_int(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_count_distinct_string(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    int64_t result = arrow_count_distinct_string(a);
    return lean_io_result_mk_ok(lean_int64_to_int(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_any(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    bool result = arrow_any(a);
    return lean_io_result_mk_ok(lean_box(result ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_arrow_all(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    bool result = arrow_all(a);
    return lean_io_result_mk_ok(lean_box(result ? 1 : 0));
}

// ============================================================================
// String Operations
// ============================================================================

LEAN_EXPORT lean_obj_res lean_arrow_string_length(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_string_length(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_substring(b_lean_obj_arg a_ptr, int32_t start, int32_t length, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_substring(a, start, length);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_string_contains(b_lean_obj_arg a_ptr, b_lean_obj_arg pattern, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    const char* pat = lean_string_cstr(pattern);
    struct ArrowArray* result = arrow_string_contains(a, pat);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_string_starts_with(b_lean_obj_arg a_ptr, b_lean_obj_arg prefix, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    const char* pref = lean_string_cstr(prefix);
    struct ArrowArray* result = arrow_string_starts_with(a, pref);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_string_ends_with(b_lean_obj_arg a_ptr, b_lean_obj_arg suffix, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    const char* suf = lean_string_cstr(suffix);
    struct ArrowArray* result = arrow_string_ends_with(a, suf);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_string_upper(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_string_upper(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_string_lower(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_string_lower(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_string_trim(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_string_trim(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

// ============================================================================
// Cast Operations
// ============================================================================

LEAN_EXPORT lean_obj_res lean_arrow_cast_int64_to_int32(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_int64_to_int32(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_int32_to_int64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_int32_to_int64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_float64_to_float32(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_float64_to_float32(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_float32_to_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_float32_to_float64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_int64_to_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_int64_to_float64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_int32_to_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_int32_to_float64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_float64_to_int64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_float64_to_int64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_int64_to_string(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_int64_to_string(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_float64_to_string(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_float64_to_string(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_bool_to_string(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_bool_to_string(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_string_to_int64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_string_to_int64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_cast_string_to_float64(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_cast_string_to_float64(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

// ============================================================================
// Filter/Take/Sort Operations
// ============================================================================

LEAN_EXPORT lean_obj_res lean_arrow_filter_int64(b_lean_obj_arg values_ptr, b_lean_obj_arg mask_ptr, lean_obj_arg w) {
    struct ArrowArray* values = (struct ArrowArray*)lean_get_external_data(values_ptr);
    struct ArrowArray* mask = (struct ArrowArray*)lean_get_external_data(mask_ptr);
    struct ArrowArray* result = arrow_filter_int64(values, mask);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_filter_float64(b_lean_obj_arg values_ptr, b_lean_obj_arg mask_ptr, lean_obj_arg w) {
    struct ArrowArray* values = (struct ArrowArray*)lean_get_external_data(values_ptr);
    struct ArrowArray* mask = (struct ArrowArray*)lean_get_external_data(mask_ptr);
    struct ArrowArray* result = arrow_filter_float64(values, mask);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_filter_string(b_lean_obj_arg values_ptr, b_lean_obj_arg mask_ptr, lean_obj_arg w) {
    struct ArrowArray* values = (struct ArrowArray*)lean_get_external_data(values_ptr);
    struct ArrowArray* mask = (struct ArrowArray*)lean_get_external_data(mask_ptr);
    struct ArrowArray* result = arrow_filter_string(values, mask);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_filter_bool(b_lean_obj_arg values_ptr, b_lean_obj_arg mask_ptr, lean_obj_arg w) {
    struct ArrowArray* values = (struct ArrowArray*)lean_get_external_data(values_ptr);
    struct ArrowArray* mask = (struct ArrowArray*)lean_get_external_data(mask_ptr);
    struct ArrowArray* result = arrow_filter_bool(values, mask);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_take_int64(b_lean_obj_arg values_ptr, b_lean_obj_arg indices_ptr, lean_obj_arg w) {
    struct ArrowArray* values = (struct ArrowArray*)lean_get_external_data(values_ptr);
    struct ArrowArray* indices = (struct ArrowArray*)lean_get_external_data(indices_ptr);
    struct ArrowArray* result = arrow_take_int64(values, indices);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_take_float64(b_lean_obj_arg values_ptr, b_lean_obj_arg indices_ptr, lean_obj_arg w) {
    struct ArrowArray* values = (struct ArrowArray*)lean_get_external_data(values_ptr);
    struct ArrowArray* indices = (struct ArrowArray*)lean_get_external_data(indices_ptr);
    struct ArrowArray* result = arrow_take_float64(values, indices);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_take_string(b_lean_obj_arg values_ptr, b_lean_obj_arg indices_ptr, lean_obj_arg w) {
    struct ArrowArray* values = (struct ArrowArray*)lean_get_external_data(values_ptr);
    struct ArrowArray* indices = (struct ArrowArray*)lean_get_external_data(indices_ptr);
    struct ArrowArray* result = arrow_take_string(values, indices);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_sort_indices_int64(b_lean_obj_arg values_ptr, uint8_t ascending, lean_obj_arg w) {
    struct ArrowArray* values = (struct ArrowArray*)lean_get_external_data(values_ptr);
    struct ArrowArray* result = arrow_sort_indices_int64(values, ascending != 0);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_sort_indices_float64(b_lean_obj_arg values_ptr, uint8_t ascending, lean_obj_arg w) {
    struct ArrowArray* values = (struct ArrowArray*)lean_get_external_data(values_ptr);
    struct ArrowArray* result = arrow_sort_indices_float64(values, ascending != 0);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_sort_indices_string(b_lean_obj_arg values_ptr, uint8_t ascending, lean_obj_arg w) {
    struct ArrowArray* values = (struct ArrowArray*)lean_get_external_data(values_ptr);
    struct ArrowArray* result = arrow_sort_indices_string(values, ascending != 0);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

// ============================================================================
// Null Handling
// ============================================================================

LEAN_EXPORT lean_obj_res lean_arrow_is_null(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_is_null(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_is_valid(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_is_valid(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_fill_null_int64(b_lean_obj_arg a_ptr, int64_t fill_value, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_fill_null_int64(a, fill_value);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_fill_null_float64(b_lean_obj_arg a_ptr, double fill_value, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_fill_null_float64(a, fill_value);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_fill_null_string(b_lean_obj_arg a_ptr, b_lean_obj_arg fill_value, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    const char* fill = lean_string_cstr(fill_value);
    struct ArrowArray* result = arrow_fill_null_string(a, fill);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

LEAN_EXPORT lean_obj_res lean_arrow_drop_null(b_lean_obj_arg a_ptr, lean_obj_arg w) {
    struct ArrowArray* a = (struct ArrowArray*)lean_get_external_data(a_ptr);
    struct ArrowArray* result = arrow_drop_null(a);
    return lean_io_result_mk_ok(wrap_array_result(result));
}

// ============================================================================
// Utility
// ============================================================================

LEAN_EXPORT lean_obj_res lean_arrow_compute_array_free(lean_obj_arg array_ptr, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_get_external_data(array_ptr);
    arrow_compute_array_free(array);
    return lean_io_result_mk_ok(lean_box(0));
}

#ifdef __cplusplus
}
#endif
