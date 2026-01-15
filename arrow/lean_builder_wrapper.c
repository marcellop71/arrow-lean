#include <lean/lean.h>
#include "arrow_builders.h"
#include "arrow_nested_builders.h"
#include <string.h>

// ============================================================================
// Lean Option Helpers
// ============================================================================

static inline lean_obj_res lean_mk_option_none(void) {
    return lean_alloc_ctor(0, 0, 0);
}

static inline lean_obj_res lean_mk_option_some(lean_obj_arg value) {
    lean_object* option = lean_alloc_ctor(1, 1, 0);
    lean_ctor_set(option, 0, value);
    return option;
}

// ============================================================================
// Int64 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_int64_builder_create(size_t capacity, lean_obj_arg w) {
    Int64Builder* builder = int64_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_int64_builder_append(b_lean_obj_arg builder_ptr, int64_t value, lean_obj_arg w) {
    Int64Builder* builder = (Int64Builder*)lean_unbox_usize(builder_ptr);
    int result = int64_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_int64_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int64Builder* builder = (Int64Builder*)lean_unbox_usize(builder_ptr);
    int result = int64_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_int64_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int64Builder* builder = (Int64Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = int64_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_int64_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int64Builder* builder = (Int64Builder*)lean_unbox_usize(builder_ptr);
    int64_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_int64_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int64Builder* builder = (Int64Builder*)lean_unbox_usize(builder_ptr);
    size_t len = int64_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Float64 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_float64_builder_create(size_t capacity, lean_obj_arg w) {
    Float64Builder* builder = float64_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_float64_builder_append(b_lean_obj_arg builder_ptr, double value, lean_obj_arg w) {
    Float64Builder* builder = (Float64Builder*)lean_unbox_usize(builder_ptr);
    int result = float64_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_float64_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Float64Builder* builder = (Float64Builder*)lean_unbox_usize(builder_ptr);
    int result = float64_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_float64_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Float64Builder* builder = (Float64Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = float64_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_float64_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Float64Builder* builder = (Float64Builder*)lean_unbox_usize(builder_ptr);
    float64_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_float64_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Float64Builder* builder = (Float64Builder*)lean_unbox_usize(builder_ptr);
    size_t len = float64_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// String Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_string_builder_create(size_t capacity, size_t data_capacity, lean_obj_arg w) {
    StringBuilder* builder = string_builder_create(capacity, data_capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_string_builder_append(b_lean_obj_arg builder_ptr, b_lean_obj_arg str, lean_obj_arg w) {
    StringBuilder* builder = (StringBuilder*)lean_unbox_usize(builder_ptr);
    const char* cstr = lean_string_cstr(str);
    size_t len = lean_string_size(str) - 1;  // Exclude null terminator
    int result = string_builder_append(builder, cstr, len);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_string_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StringBuilder* builder = (StringBuilder*)lean_unbox_usize(builder_ptr);
    int result = string_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_string_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StringBuilder* builder = (StringBuilder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = string_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_string_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StringBuilder* builder = (StringBuilder*)lean_unbox_usize(builder_ptr);
    string_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_string_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StringBuilder* builder = (StringBuilder*)lean_unbox_usize(builder_ptr);
    size_t len = string_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Timestamp Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_timestamp_builder_create(size_t capacity, b_lean_obj_arg timezone, lean_obj_arg w) {
    const char* tz = lean_string_cstr(timezone);
    TimestampBuilder* builder = timestamp_builder_create(capacity, tz);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_timestamp_builder_append(b_lean_obj_arg builder_ptr, int64_t microseconds, lean_obj_arg w) {
    TimestampBuilder* builder = (TimestampBuilder*)lean_unbox_usize(builder_ptr);
    int result = timestamp_builder_append(builder, microseconds);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_timestamp_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    TimestampBuilder* builder = (TimestampBuilder*)lean_unbox_usize(builder_ptr);
    int result = timestamp_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_timestamp_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    TimestampBuilder* builder = (TimestampBuilder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = timestamp_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_timestamp_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    TimestampBuilder* builder = (TimestampBuilder*)lean_unbox_usize(builder_ptr);
    timestamp_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_timestamp_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    TimestampBuilder* builder = (TimestampBuilder*)lean_unbox_usize(builder_ptr);
    size_t len = timestamp_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

LEAN_EXPORT lean_obj_res lean_timestamp_builder_get_timezone(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    TimestampBuilder* builder = (TimestampBuilder*)lean_unbox_usize(builder_ptr);
    const char* tz = timestamp_builder_get_timezone(builder);
    if (!tz) {
        return lean_io_result_mk_ok(lean_mk_string(""));
    }
    return lean_io_result_mk_ok(lean_mk_string(tz));
}

// ============================================================================
// Bool Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_bool_builder_create(size_t capacity, lean_obj_arg w) {
    BoolBuilder* builder = bool_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_bool_builder_append(b_lean_obj_arg builder_ptr, uint8_t value, lean_obj_arg w) {
    BoolBuilder* builder = (BoolBuilder*)lean_unbox_usize(builder_ptr);
    int result = bool_builder_append(builder, value != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_bool_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    BoolBuilder* builder = (BoolBuilder*)lean_unbox_usize(builder_ptr);
    int result = bool_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_bool_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    BoolBuilder* builder = (BoolBuilder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = bool_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_bool_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    BoolBuilder* builder = (BoolBuilder*)lean_unbox_usize(builder_ptr);
    bool_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_bool_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    BoolBuilder* builder = (BoolBuilder*)lean_unbox_usize(builder_ptr);
    size_t len = bool_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Int8 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_int8_builder_create(size_t capacity, lean_obj_arg w) {
    Int8Builder* builder = int8_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_int8_builder_append(b_lean_obj_arg builder_ptr, int8_t value, lean_obj_arg w) {
    Int8Builder* builder = (Int8Builder*)lean_unbox_usize(builder_ptr);
    int result = int8_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_int8_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int8Builder* builder = (Int8Builder*)lean_unbox_usize(builder_ptr);
    int result = int8_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_int8_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int8Builder* builder = (Int8Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = int8_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_int8_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int8Builder* builder = (Int8Builder*)lean_unbox_usize(builder_ptr);
    int8_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_int8_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int8Builder* builder = (Int8Builder*)lean_unbox_usize(builder_ptr);
    size_t len = int8_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Int16 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_int16_builder_create(size_t capacity, lean_obj_arg w) {
    Int16Builder* builder = int16_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_int16_builder_append(b_lean_obj_arg builder_ptr, int16_t value, lean_obj_arg w) {
    Int16Builder* builder = (Int16Builder*)lean_unbox_usize(builder_ptr);
    int result = int16_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_int16_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int16Builder* builder = (Int16Builder*)lean_unbox_usize(builder_ptr);
    int result = int16_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_int16_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int16Builder* builder = (Int16Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = int16_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_int16_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int16Builder* builder = (Int16Builder*)lean_unbox_usize(builder_ptr);
    int16_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_int16_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int16Builder* builder = (Int16Builder*)lean_unbox_usize(builder_ptr);
    size_t len = int16_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Int32 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_int32_builder_create(size_t capacity, lean_obj_arg w) {
    Int32Builder* builder = int32_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_int32_builder_append(b_lean_obj_arg builder_ptr, int32_t value, lean_obj_arg w) {
    Int32Builder* builder = (Int32Builder*)lean_unbox_usize(builder_ptr);
    int result = int32_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_int32_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int32Builder* builder = (Int32Builder*)lean_unbox_usize(builder_ptr);
    int result = int32_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_int32_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int32Builder* builder = (Int32Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = int32_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_int32_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int32Builder* builder = (Int32Builder*)lean_unbox_usize(builder_ptr);
    int32_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_int32_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Int32Builder* builder = (Int32Builder*)lean_unbox_usize(builder_ptr);
    size_t len = int32_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// UInt8 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_uint8_builder_create(size_t capacity, lean_obj_arg w) {
    UInt8Builder* builder = uint8_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_uint8_builder_append(b_lean_obj_arg builder_ptr, uint8_t value, lean_obj_arg w) {
    UInt8Builder* builder = (UInt8Builder*)lean_unbox_usize(builder_ptr);
    int result = uint8_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_uint8_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt8Builder* builder = (UInt8Builder*)lean_unbox_usize(builder_ptr);
    int result = uint8_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_uint8_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt8Builder* builder = (UInt8Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = uint8_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_uint8_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt8Builder* builder = (UInt8Builder*)lean_unbox_usize(builder_ptr);
    uint8_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_uint8_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt8Builder* builder = (UInt8Builder*)lean_unbox_usize(builder_ptr);
    size_t len = uint8_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// UInt16 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_uint16_builder_create(size_t capacity, lean_obj_arg w) {
    UInt16Builder* builder = uint16_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_uint16_builder_append(b_lean_obj_arg builder_ptr, uint16_t value, lean_obj_arg w) {
    UInt16Builder* builder = (UInt16Builder*)lean_unbox_usize(builder_ptr);
    int result = uint16_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_uint16_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt16Builder* builder = (UInt16Builder*)lean_unbox_usize(builder_ptr);
    int result = uint16_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_uint16_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt16Builder* builder = (UInt16Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = uint16_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_uint16_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt16Builder* builder = (UInt16Builder*)lean_unbox_usize(builder_ptr);
    uint16_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_uint16_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt16Builder* builder = (UInt16Builder*)lean_unbox_usize(builder_ptr);
    size_t len = uint16_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// UInt32 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_uint32_builder_create(size_t capacity, lean_obj_arg w) {
    UInt32Builder* builder = uint32_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_uint32_builder_append(b_lean_obj_arg builder_ptr, uint32_t value, lean_obj_arg w) {
    UInt32Builder* builder = (UInt32Builder*)lean_unbox_usize(builder_ptr);
    int result = uint32_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_uint32_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt32Builder* builder = (UInt32Builder*)lean_unbox_usize(builder_ptr);
    int result = uint32_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_uint32_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt32Builder* builder = (UInt32Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = uint32_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_uint32_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt32Builder* builder = (UInt32Builder*)lean_unbox_usize(builder_ptr);
    uint32_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_uint32_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt32Builder* builder = (UInt32Builder*)lean_unbox_usize(builder_ptr);
    size_t len = uint32_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// UInt64 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_uint64_builder_create(size_t capacity, lean_obj_arg w) {
    UInt64Builder* builder = uint64_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_uint64_builder_append(b_lean_obj_arg builder_ptr, uint64_t value, lean_obj_arg w) {
    UInt64Builder* builder = (UInt64Builder*)lean_unbox_usize(builder_ptr);
    int result = uint64_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_uint64_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt64Builder* builder = (UInt64Builder*)lean_unbox_usize(builder_ptr);
    int result = uint64_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_uint64_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt64Builder* builder = (UInt64Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = uint64_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_uint64_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt64Builder* builder = (UInt64Builder*)lean_unbox_usize(builder_ptr);
    uint64_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_uint64_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    UInt64Builder* builder = (UInt64Builder*)lean_unbox_usize(builder_ptr);
    size_t len = uint64_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Float32 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_float32_builder_create(size_t capacity, lean_obj_arg w) {
    Float32Builder* builder = float32_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_float32_builder_append(b_lean_obj_arg builder_ptr, double value, lean_obj_arg w) {
    Float32Builder* builder = (Float32Builder*)lean_unbox_usize(builder_ptr);
    int result = float32_builder_append(builder, (float)value);  // Cast from Float (double in Lean)
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_float32_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Float32Builder* builder = (Float32Builder*)lean_unbox_usize(builder_ptr);
    int result = float32_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_float32_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Float32Builder* builder = (Float32Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = float32_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_float32_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Float32Builder* builder = (Float32Builder*)lean_unbox_usize(builder_ptr);
    float32_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_float32_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Float32Builder* builder = (Float32Builder*)lean_unbox_usize(builder_ptr);
    size_t len = float32_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Date32 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_date32_builder_create(size_t capacity, lean_obj_arg w) {
    Date32Builder* builder = date32_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_date32_builder_append(b_lean_obj_arg builder_ptr, int32_t days, lean_obj_arg w) {
    Date32Builder* builder = (Date32Builder*)lean_unbox_usize(builder_ptr);
    int result = date32_builder_append(builder, days);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_date32_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Date32Builder* builder = (Date32Builder*)lean_unbox_usize(builder_ptr);
    int result = date32_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_date32_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Date32Builder* builder = (Date32Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = date32_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_date32_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Date32Builder* builder = (Date32Builder*)lean_unbox_usize(builder_ptr);
    date32_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_date32_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Date32Builder* builder = (Date32Builder*)lean_unbox_usize(builder_ptr);
    size_t len = date32_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Date64 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_date64_builder_create(size_t capacity, lean_obj_arg w) {
    Date64Builder* builder = date64_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_date64_builder_append(b_lean_obj_arg builder_ptr, int64_t milliseconds, lean_obj_arg w) {
    Date64Builder* builder = (Date64Builder*)lean_unbox_usize(builder_ptr);
    int result = date64_builder_append(builder, milliseconds);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_date64_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Date64Builder* builder = (Date64Builder*)lean_unbox_usize(builder_ptr);
    int result = date64_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_date64_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Date64Builder* builder = (Date64Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = date64_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_date64_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Date64Builder* builder = (Date64Builder*)lean_unbox_usize(builder_ptr);
    date64_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_date64_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Date64Builder* builder = (Date64Builder*)lean_unbox_usize(builder_ptr);
    size_t len = date64_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Time32 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_time32_builder_create(size_t capacity, uint8_t unit, lean_obj_arg w) {
    Time32Builder* builder = time32_builder_create(capacity, (char)unit);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_time32_builder_append(b_lean_obj_arg builder_ptr, int32_t value, lean_obj_arg w) {
    Time32Builder* builder = (Time32Builder*)lean_unbox_usize(builder_ptr);
    int result = time32_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_time32_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Time32Builder* builder = (Time32Builder*)lean_unbox_usize(builder_ptr);
    int result = time32_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_time32_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Time32Builder* builder = (Time32Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = time32_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_time32_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Time32Builder* builder = (Time32Builder*)lean_unbox_usize(builder_ptr);
    time32_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_time32_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Time32Builder* builder = (Time32Builder*)lean_unbox_usize(builder_ptr);
    size_t len = time32_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Time64 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_time64_builder_create(size_t capacity, uint8_t unit, lean_obj_arg w) {
    Time64Builder* builder = time64_builder_create(capacity, (char)unit);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_time64_builder_append(b_lean_obj_arg builder_ptr, int64_t value, lean_obj_arg w) {
    Time64Builder* builder = (Time64Builder*)lean_unbox_usize(builder_ptr);
    int result = time64_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_time64_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Time64Builder* builder = (Time64Builder*)lean_unbox_usize(builder_ptr);
    int result = time64_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_time64_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Time64Builder* builder = (Time64Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = time64_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_time64_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Time64Builder* builder = (Time64Builder*)lean_unbox_usize(builder_ptr);
    time64_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_time64_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Time64Builder* builder = (Time64Builder*)lean_unbox_usize(builder_ptr);
    size_t len = time64_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Duration Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_duration_builder_create(size_t capacity, uint8_t unit, lean_obj_arg w) {
    DurationBuilder* builder = duration_builder_create(capacity, (char)unit);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_duration_builder_append(b_lean_obj_arg builder_ptr, int64_t value, lean_obj_arg w) {
    DurationBuilder* builder = (DurationBuilder*)lean_unbox_usize(builder_ptr);
    int result = duration_builder_append(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_duration_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    DurationBuilder* builder = (DurationBuilder*)lean_unbox_usize(builder_ptr);
    int result = duration_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_duration_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    DurationBuilder* builder = (DurationBuilder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = duration_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_duration_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    DurationBuilder* builder = (DurationBuilder*)lean_unbox_usize(builder_ptr);
    duration_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_duration_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    DurationBuilder* builder = (DurationBuilder*)lean_unbox_usize(builder_ptr);
    size_t len = duration_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Binary Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_binary_builder_create(size_t capacity, size_t data_capacity, lean_obj_arg w) {
    BinaryBuilder* builder = binary_builder_create(capacity, data_capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_binary_builder_append(b_lean_obj_arg builder_ptr, b_lean_obj_arg data, lean_obj_arg w) {
    BinaryBuilder* builder = (BinaryBuilder*)lean_unbox_usize(builder_ptr);
    size_t len = lean_sarray_size(data);
    const uint8_t* bytes = lean_sarray_cptr(data);
    int result = binary_builder_append(builder, bytes, len);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_binary_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    BinaryBuilder* builder = (BinaryBuilder*)lean_unbox_usize(builder_ptr);
    int result = binary_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_binary_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    BinaryBuilder* builder = (BinaryBuilder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = binary_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_binary_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    BinaryBuilder* builder = (BinaryBuilder*)lean_unbox_usize(builder_ptr);
    binary_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_binary_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    BinaryBuilder* builder = (BinaryBuilder*)lean_unbox_usize(builder_ptr);
    size_t len = binary_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Schema Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_schema_builder_create(size_t capacity, lean_obj_arg w) {
    SchemaBuilder* builder = schema_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_int64(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_int64(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_float64(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_float64(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_string(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_string(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_timestamp(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, b_lean_obj_arg timezone, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    const char* tz = lean_string_cstr(timezone);
    int result = schema_builder_add_timestamp_us(builder, cname, tz, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_bool(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_bool(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_int8(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_int8(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_int16(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_int16(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_int32(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_int32(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_uint8(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_uint8(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_uint16(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_uint16(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_uint32(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_uint32(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_uint64(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_uint64(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_float32(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_float32(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_date32(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_date32(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_date64(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_date64(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_time32(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t unit, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_time32(builder, cname, (char)unit, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_time64(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t unit, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_time64(builder, cname, (char)unit, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_duration(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t unit, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_duration(builder, cname, (char)unit, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_add_binary(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t nullable, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = schema_builder_add_binary(builder, cname, nullable != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    struct ArrowSchema* schema = schema_builder_finish(builder);
    if (!schema) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)schema)));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    schema_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_schema_builder_field_count(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    SchemaBuilder* builder = (SchemaBuilder*)lean_unbox_usize(builder_ptr);
    size_t count = schema_builder_field_count(builder);
    return lean_io_result_mk_ok(lean_box_usize(count));
}

// ============================================================================
// RecordBatch FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_record_batch_create(
    b_lean_obj_arg schema_ptr,
    b_lean_obj_arg columns_array,
    size_t num_rows,
    lean_obj_arg w
) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr);

    // Convert Lean array to C array of ArrowArray pointers
    size_t num_columns = lean_array_size(columns_array);
    struct ArrowArray** columns = NULL;

    if (num_columns > 0) {
        columns = malloc(num_columns * sizeof(struct ArrowArray*));
        if (!columns) {
            return lean_io_result_mk_ok(lean_mk_option_none());
        }

        for (size_t i = 0; i < num_columns; i++) {
            lean_object* elem = lean_array_get_core(columns_array, i);
            columns[i] = (struct ArrowArray*)lean_unbox_usize(elem);
        }
    }

    RecordBatch* batch = record_batch_create(schema, columns, num_columns, num_rows);
    if (!batch) {
        free(columns);
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)batch)));
}

LEAN_EXPORT lean_obj_res lean_record_batch_num_rows(b_lean_obj_arg batch_ptr, lean_obj_arg w) {
    RecordBatch* batch = (RecordBatch*)lean_unbox_usize(batch_ptr);
    size_t rows = record_batch_num_rows(batch);
    return lean_io_result_mk_ok(lean_box_usize(rows));
}

LEAN_EXPORT lean_obj_res lean_record_batch_num_columns(b_lean_obj_arg batch_ptr, lean_obj_arg w) {
    RecordBatch* batch = (RecordBatch*)lean_unbox_usize(batch_ptr);
    size_t cols = record_batch_num_columns(batch);
    return lean_io_result_mk_ok(lean_box_usize(cols));
}

LEAN_EXPORT lean_obj_res lean_record_batch_to_struct_array(b_lean_obj_arg batch_ptr, lean_obj_arg w) {
    RecordBatch* batch = (RecordBatch*)lean_unbox_usize(batch_ptr);
    struct ArrowArray* array = record_batch_to_struct_array(batch);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_record_batch_free(b_lean_obj_arg batch_ptr, lean_obj_arg w) {
    RecordBatch* batch = (RecordBatch*)lean_unbox_usize(batch_ptr);
    record_batch_free(batch);
    return lean_io_result_mk_ok(lean_box(0));
}

// ============================================================================
// Stream FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_batch_to_stream(b_lean_obj_arg batch_ptr, lean_obj_arg w) {
    RecordBatch* batch = (RecordBatch*)lean_unbox_usize(batch_ptr);
    struct ArrowArrayStream* stream = batch_to_stream(batch);
    if (!stream) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)stream)));
}

LEAN_EXPORT lean_obj_res lean_batches_to_stream(
    b_lean_obj_arg schema_ptr,
    b_lean_obj_arg batches_array,
    lean_obj_arg w
) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr);

    size_t num_batches = lean_array_size(batches_array);
    RecordBatch** batches = NULL;

    if (num_batches > 0) {
        batches = malloc(num_batches * sizeof(RecordBatch*));
        if (!batches) {
            return lean_io_result_mk_ok(lean_mk_option_none());
        }

        for (size_t i = 0; i < num_batches; i++) {
            lean_object* elem = lean_array_get_core(batches_array, i);
            batches[i] = (RecordBatch*)lean_unbox_usize(elem);
        }
    }

    struct ArrowArrayStream* stream = batches_to_stream(schema, batches, num_batches);
    if (!stream) {
        free(batches);
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)stream)));
}

// ============================================================================
// List Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_list_builder_create(size_t capacity, uint8_t element_type, lean_obj_arg w) {
    ListBuilder* builder = list_builder_create(capacity, (ListElementType)element_type);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_list_builder_start_list(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    ListBuilder* builder = (ListBuilder*)lean_unbox_usize(builder_ptr);
    int result = list_builder_start_list(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_list_builder_finish_list(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    ListBuilder* builder = (ListBuilder*)lean_unbox_usize(builder_ptr);
    int result = list_builder_finish_list(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_list_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    ListBuilder* builder = (ListBuilder*)lean_unbox_usize(builder_ptr);
    int result = list_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_list_builder_append_int64(b_lean_obj_arg builder_ptr, int64_t value, lean_obj_arg w) {
    ListBuilder* builder = (ListBuilder*)lean_unbox_usize(builder_ptr);
    int result = list_builder_append_int64(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_list_builder_append_float64(b_lean_obj_arg builder_ptr, double value, lean_obj_arg w) {
    ListBuilder* builder = (ListBuilder*)lean_unbox_usize(builder_ptr);
    int result = list_builder_append_float64(builder, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_list_builder_append_string(b_lean_obj_arg builder_ptr, b_lean_obj_arg value, lean_obj_arg w) {
    ListBuilder* builder = (ListBuilder*)lean_unbox_usize(builder_ptr);
    const char* cvalue = lean_string_cstr(value);
    int result = list_builder_append_string(builder, cvalue);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_list_builder_append_bool(b_lean_obj_arg builder_ptr, uint8_t value, lean_obj_arg w) {
    ListBuilder* builder = (ListBuilder*)lean_unbox_usize(builder_ptr);
    int result = list_builder_append_bool(builder, value != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_list_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    ListBuilder* builder = (ListBuilder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = list_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_list_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    ListBuilder* builder = (ListBuilder*)lean_unbox_usize(builder_ptr);
    list_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_list_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    ListBuilder* builder = (ListBuilder*)lean_unbox_usize(builder_ptr);
    size_t len = list_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

// ============================================================================
// Struct Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_struct_builder_create(size_t capacity, lean_obj_arg w) {
    StructBuilder* builder = struct_builder_create(capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_add_field(b_lean_obj_arg builder_ptr, b_lean_obj_arg name, uint8_t field_type, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    const char* cname = lean_string_cstr(name);
    int result = struct_builder_add_field(builder, cname, (ListElementType)field_type);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_append_int64(b_lean_obj_arg builder_ptr, size_t field_idx, int64_t value, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    int result = struct_builder_append_int64(builder, field_idx, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_append_float64(b_lean_obj_arg builder_ptr, size_t field_idx, double value, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    int result = struct_builder_append_float64(builder, field_idx, value);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_append_string(b_lean_obj_arg builder_ptr, size_t field_idx, b_lean_obj_arg value, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    const char* cvalue = lean_string_cstr(value);
    int result = struct_builder_append_string(builder, field_idx, cvalue);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_append_bool(b_lean_obj_arg builder_ptr, size_t field_idx, uint8_t value, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    int result = struct_builder_append_bool(builder, field_idx, value != 0);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_append_field_null(b_lean_obj_arg builder_ptr, size_t field_idx, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    int result = struct_builder_append_field_null(builder, field_idx);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    int result = struct_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_finish_row(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    int result = struct_builder_finish_row(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = struct_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_get_schema(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    struct ArrowSchema* schema = struct_builder_get_schema(builder);
    if (!schema) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)schema)));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    struct_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    size_t len = struct_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

LEAN_EXPORT lean_obj_res lean_struct_builder_field_count(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    StructBuilder* builder = (StructBuilder*)lean_unbox_usize(builder_ptr);
    size_t count = struct_builder_field_count(builder);
    return lean_io_result_mk_ok(lean_box_usize(count));
}

// ============================================================================
// Decimal128 Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_decimal128_builder_create(size_t capacity, int32_t precision, int32_t scale, lean_obj_arg w) {
    Decimal128Builder* builder = decimal128_builder_create(capacity, precision, scale);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_decimal128_builder_append(b_lean_obj_arg builder_ptr, int64_t high, uint64_t low, lean_obj_arg w) {
    Decimal128Builder* builder = (Decimal128Builder*)lean_unbox_usize(builder_ptr);
    int result = decimal128_builder_append(builder, high, low);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_decimal128_builder_append_string(b_lean_obj_arg builder_ptr, b_lean_obj_arg value, lean_obj_arg w) {
    Decimal128Builder* builder = (Decimal128Builder*)lean_unbox_usize(builder_ptr);
    const char* cvalue = lean_string_cstr(value);
    int result = decimal128_builder_append_string(builder, cvalue);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_decimal128_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Decimal128Builder* builder = (Decimal128Builder*)lean_unbox_usize(builder_ptr);
    int result = decimal128_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_decimal128_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Decimal128Builder* builder = (Decimal128Builder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* array = decimal128_builder_finish(builder);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_decimal128_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Decimal128Builder* builder = (Decimal128Builder*)lean_unbox_usize(builder_ptr);
    decimal128_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_decimal128_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Decimal128Builder* builder = (Decimal128Builder*)lean_unbox_usize(builder_ptr);
    size_t len = decimal128_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

LEAN_EXPORT lean_obj_res lean_decimal128_builder_precision(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Decimal128Builder* builder = (Decimal128Builder*)lean_unbox_usize(builder_ptr);
    int32_t prec = decimal128_builder_precision(builder);
    return lean_io_result_mk_ok(lean_box((unsigned)prec));
}

LEAN_EXPORT lean_obj_res lean_decimal128_builder_scale(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    Decimal128Builder* builder = (Decimal128Builder*)lean_unbox_usize(builder_ptr);
    int32_t sc = decimal128_builder_scale(builder);
    return lean_io_result_mk_ok(lean_box((unsigned)sc));
}

// ============================================================================
// Dictionary Builder FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_dictionary_builder_create(size_t capacity, size_t dict_capacity, lean_obj_arg w) {
    DictionaryBuilder* builder = dictionary_builder_create(capacity, dict_capacity);
    if (!builder) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)builder)));
}

LEAN_EXPORT lean_obj_res lean_dictionary_builder_append(b_lean_obj_arg builder_ptr, b_lean_obj_arg value, lean_obj_arg w) {
    DictionaryBuilder* builder = (DictionaryBuilder*)lean_unbox_usize(builder_ptr);
    const char* cvalue = lean_string_cstr(value);
    int result = dictionary_builder_append(builder, cvalue);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_dictionary_builder_append_null(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    DictionaryBuilder* builder = (DictionaryBuilder*)lean_unbox_usize(builder_ptr);
    int result = dictionary_builder_append_null(builder);
    return lean_io_result_mk_ok(lean_box(result == BUILDER_OK ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_dictionary_builder_finish(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    DictionaryBuilder* builder = (DictionaryBuilder*)lean_unbox_usize(builder_ptr);
    struct ArrowArray* dict = NULL;
    struct ArrowArray* array = dictionary_builder_finish(builder, &dict);
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array)));
}

LEAN_EXPORT lean_obj_res lean_dictionary_builder_free(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    DictionaryBuilder* builder = (DictionaryBuilder*)lean_unbox_usize(builder_ptr);
    dictionary_builder_free(builder);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_dictionary_builder_length(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    DictionaryBuilder* builder = (DictionaryBuilder*)lean_unbox_usize(builder_ptr);
    size_t len = dictionary_builder_length(builder);
    return lean_io_result_mk_ok(lean_box_usize(len));
}

LEAN_EXPORT lean_obj_res lean_dictionary_builder_dict_size(b_lean_obj_arg builder_ptr, lean_obj_arg w) {
    DictionaryBuilder* builder = (DictionaryBuilder*)lean_unbox_usize(builder_ptr);
    size_t size = dictionary_builder_dict_size(builder);
    return lean_io_result_mk_ok(lean_box_usize(size));
}
