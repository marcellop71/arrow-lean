#include "arrow_compute.h"
#include "arrow_builders.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <ctype.h>

// ============================================================================
// Helper Functions
// ============================================================================

// Helper to check validity bitmap
static bool is_valid_at(struct ArrowArray* a, int64_t idx) {
    if (a->null_count == 0) return true;
    if (a->buffers[0] == NULL) return true;

    const uint8_t* validity = (const uint8_t*)a->buffers[0];
    int64_t actual_idx = idx + a->offset;
    return (validity[actual_idx / 8] >> (actual_idx % 8)) & 1;
}

// Get int64 value at index
static int64_t get_int64_at(struct ArrowArray* a, int64_t idx) {
    const int64_t* data = (const int64_t*)a->buffers[1];
    return data[idx + a->offset];
}

// Get float64 value at index
static double get_float64_at(struct ArrowArray* a, int64_t idx) {
    const double* data = (const double*)a->buffers[1];
    return data[idx + a->offset];
}

// Get int32 value at index
static int32_t get_int32_at(struct ArrowArray* a, int64_t idx) {
    const int32_t* data = (const int32_t*)a->buffers[1];
    return data[idx + a->offset];
}

// Get bool value at index
static bool get_bool_at(struct ArrowArray* a, int64_t idx) {
    const uint8_t* data = (const uint8_t*)a->buffers[1];
    int64_t actual_idx = idx + a->offset;
    return (data[actual_idx / 8] >> (actual_idx % 8)) & 1;
}

// Get string at index (returns pointer and length, not a copy)
static const char* get_string_at(struct ArrowArray* a, int64_t idx, int32_t* out_len) {
    const int32_t* offsets = (const int32_t*)a->buffers[1];
    const char* data = (const char*)a->buffers[2];
    int64_t actual_idx = idx + a->offset;
    int32_t start = offsets[actual_idx];
    int32_t end = offsets[actual_idx + 1];
    *out_len = end - start;
    return data + start;
}

// Allocate validity bitmap
static uint8_t* alloc_validity(int64_t length) {
    size_t bytes = (length + 7) / 8;
    return calloc(bytes, 1);
}

// Set validity bit
static void set_valid(uint8_t* validity, int64_t idx, bool valid) {
    if (valid) {
        validity[idx / 8] |= (1 << (idx % 8));
    } else {
        validity[idx / 8] &= ~(1 << (idx % 8));
    }
}

// Release function for computed arrays
static void release_computed_array(struct ArrowArray* array) {
    if (!array) return;
    if (array->buffers) {
        for (int64_t i = 0; i < array->n_buffers; i++) {
            free((void*)array->buffers[i]);
        }
        free(array->buffers);
    }
    if (array->children) {
        for (int64_t i = 0; i < array->n_children; i++) {
            if (array->children[i] && array->children[i]->release) {
                array->children[i]->release(array->children[i]);
            }
            free(array->children[i]);
        }
        free(array->children);
    }
    array->release = NULL;
}

// Create result array for int64
static struct ArrowArray* create_int64_result(int64_t length) {
    struct ArrowArray* result = calloc(1, sizeof(struct ArrowArray));
    if (!result) return NULL;

    result->length = length;
    result->null_count = 0;
    result->offset = 0;
    result->n_buffers = 2;
    result->n_children = 0;
    result->buffers = calloc(2, sizeof(void*));
    result->buffers[0] = NULL;  // No validity bitmap initially
    result->buffers[1] = calloc(length, sizeof(int64_t));
    result->children = NULL;
    result->dictionary = NULL;
    result->release = release_computed_array;

    if (!result->buffers || !result->buffers[1]) {
        arrow_compute_array_free(result);
        return NULL;
    }
    return result;
}

// Create result array for float64
static struct ArrowArray* create_float64_result(int64_t length) {
    struct ArrowArray* result = calloc(1, sizeof(struct ArrowArray));
    if (!result) return NULL;

    result->length = length;
    result->null_count = 0;
    result->offset = 0;
    result->n_buffers = 2;
    result->n_children = 0;
    result->buffers = calloc(2, sizeof(void*));
    result->buffers[0] = NULL;
    result->buffers[1] = calloc(length, sizeof(double));
    result->children = NULL;
    result->dictionary = NULL;
    result->release = release_computed_array;

    if (!result->buffers || !result->buffers[1]) {
        arrow_compute_array_free(result);
        return NULL;
    }
    return result;
}

// Create result array for bool
static struct ArrowArray* create_bool_result(int64_t length) {
    struct ArrowArray* result = calloc(1, sizeof(struct ArrowArray));
    if (!result) return NULL;

    size_t bytes = (length + 7) / 8;
    result->length = length;
    result->null_count = 0;
    result->offset = 0;
    result->n_buffers = 2;
    result->n_children = 0;
    result->buffers = calloc(2, sizeof(void*));
    result->buffers[0] = NULL;
    result->buffers[1] = calloc(bytes, 1);
    result->children = NULL;
    result->dictionary = NULL;
    result->release = release_computed_array;

    if (!result->buffers || !result->buffers[1]) {
        arrow_compute_array_free(result);
        return NULL;
    }
    return result;
}

// Set bool value in result array
static void set_bool_result(struct ArrowArray* result, int64_t idx, bool value) {
    uint8_t* data = (uint8_t*)result->buffers[1];
    if (value) {
        data[idx / 8] |= (1 << (idx % 8));
    } else {
        data[idx / 8] &= ~(1 << (idx % 8));
    }
}

// ============================================================================
// Arithmetic Operations Implementation
// ============================================================================

struct ArrowArray* arrow_add_int64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    int64_t null_count = 0;
    uint8_t* validity = NULL;

    for (int64_t i = 0; i < a->length; i++) {
        bool valid_a = is_valid_at(a, i);
        bool valid_b = is_valid_at(b, i);
        if (valid_a && valid_b) {
            out[i] = get_int64_at(a, i) + get_int64_at(b, i);
        } else {
            out[i] = 0;
            null_count++;
            if (!validity) {
                validity = alloc_validity(a->length);
                for (int64_t j = 0; j < i; j++) set_valid(validity, j, true);
                result->buffers[0] = validity;
            }
            set_valid(validity, i, false);
        }
        if (validity && (valid_a && valid_b)) {
            set_valid(validity, i, true);
        }
    }

    result->null_count = null_count;
    return result;
}

struct ArrowArray* arrow_add_float64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    int64_t null_count = 0;
    uint8_t* validity = NULL;

    for (int64_t i = 0; i < a->length; i++) {
        bool valid_a = is_valid_at(a, i);
        bool valid_b = is_valid_at(b, i);
        if (valid_a && valid_b) {
            out[i] = get_float64_at(a, i) + get_float64_at(b, i);
        } else {
            out[i] = 0.0;
            null_count++;
            if (!validity) {
                validity = alloc_validity(a->length);
                for (int64_t j = 0; j < i; j++) set_valid(validity, j, true);
                result->buffers[0] = validity;
            }
            set_valid(validity, i, false);
        }
        if (validity && (valid_a && valid_b)) {
            set_valid(validity, i, true);
        }
    }

    result->null_count = null_count;
    return result;
}

struct ArrowArray* arrow_subtract_int64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            out[i] = get_int64_at(a, i) - get_int64_at(b, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_subtract_float64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            out[i] = get_float64_at(a, i) - get_float64_at(b, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_multiply_int64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            out[i] = get_int64_at(a, i) * get_int64_at(b, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_multiply_float64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            out[i] = get_float64_at(a, i) * get_float64_at(b, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_divide_int64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            int64_t divisor = get_int64_at(b, i);
            out[i] = divisor != 0 ? get_int64_at(a, i) / divisor : 0;
        }
    }
    return result;
}

struct ArrowArray* arrow_divide_float64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            out[i] = get_float64_at(a, i) / get_float64_at(b, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_add_scalar_int64(struct ArrowArray* a, int64_t scalar) {
    if (!a) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = get_int64_at(a, i) + scalar;
        }
    }
    return result;
}

struct ArrowArray* arrow_add_scalar_float64(struct ArrowArray* a, double scalar) {
    if (!a) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = get_float64_at(a, i) + scalar;
        }
    }
    return result;
}

struct ArrowArray* arrow_multiply_scalar_int64(struct ArrowArray* a, int64_t scalar) {
    if (!a) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = get_int64_at(a, i) * scalar;
        }
    }
    return result;
}

struct ArrowArray* arrow_multiply_scalar_float64(struct ArrowArray* a, double scalar) {
    if (!a) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = get_float64_at(a, i) * scalar;
        }
    }
    return result;
}

struct ArrowArray* arrow_negate_int64(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = -get_int64_at(a, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_negate_float64(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = -get_float64_at(a, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_abs_int64(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            int64_t val = get_int64_at(a, i);
            out[i] = val < 0 ? -val : val;
        }
    }
    return result;
}

struct ArrowArray* arrow_abs_float64(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = fabs(get_float64_at(a, i));
        }
    }
    return result;
}

// ============================================================================
// Comparison Operations Implementation
// ============================================================================

struct ArrowArray* arrow_eq_int64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_int64_at(a, i) == get_int64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_eq_float64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_float64_at(a, i) == get_float64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_eq_string(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            int32_t len_a, len_b;
            const char* str_a = get_string_at(a, i, &len_a);
            const char* str_b = get_string_at(b, i, &len_b);
            bool eq = (len_a == len_b) && (memcmp(str_a, str_b, len_a) == 0);
            set_bool_result(result, i, eq);
        }
    }
    return result;
}

struct ArrowArray* arrow_ne_int64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_int64_at(a, i) != get_int64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_ne_float64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_float64_at(a, i) != get_float64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_lt_int64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_int64_at(a, i) < get_int64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_lt_float64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_float64_at(a, i) < get_float64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_le_int64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_int64_at(a, i) <= get_int64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_le_float64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_float64_at(a, i) <= get_float64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_gt_int64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_int64_at(a, i) > get_int64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_gt_float64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_float64_at(a, i) > get_float64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_ge_int64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_int64_at(a, i) >= get_int64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_ge_float64(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && is_valid_at(b, i)) {
            set_bool_result(result, i, get_float64_at(a, i) >= get_float64_at(b, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_eq_scalar_int64(struct ArrowArray* a, int64_t scalar) {
    if (!a) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            set_bool_result(result, i, get_int64_at(a, i) == scalar);
        }
    }
    return result;
}

struct ArrowArray* arrow_lt_scalar_int64(struct ArrowArray* a, int64_t scalar) {
    if (!a) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            set_bool_result(result, i, get_int64_at(a, i) < scalar);
        }
    }
    return result;
}

struct ArrowArray* arrow_gt_scalar_int64(struct ArrowArray* a, int64_t scalar) {
    if (!a) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            set_bool_result(result, i, get_int64_at(a, i) > scalar);
        }
    }
    return result;
}

struct ArrowArray* arrow_eq_scalar_float64(struct ArrowArray* a, double scalar) {
    if (!a) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            set_bool_result(result, i, get_float64_at(a, i) == scalar);
        }
    }
    return result;
}

struct ArrowArray* arrow_lt_scalar_float64(struct ArrowArray* a, double scalar) {
    if (!a) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            set_bool_result(result, i, get_float64_at(a, i) < scalar);
        }
    }
    return result;
}

struct ArrowArray* arrow_gt_scalar_float64(struct ArrowArray* a, double scalar) {
    if (!a) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            set_bool_result(result, i, get_float64_at(a, i) > scalar);
        }
    }
    return result;
}

// ============================================================================
// Logical Operations Implementation
// ============================================================================

struct ArrowArray* arrow_and(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        bool val_a = is_valid_at(a, i) && get_bool_at(a, i);
        bool val_b = is_valid_at(b, i) && get_bool_at(b, i);
        set_bool_result(result, i, val_a && val_b);
    }
    return result;
}

struct ArrowArray* arrow_or(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        bool val_a = is_valid_at(a, i) && get_bool_at(a, i);
        bool val_b = is_valid_at(b, i) && get_bool_at(b, i);
        set_bool_result(result, i, val_a || val_b);
    }
    return result;
}

struct ArrowArray* arrow_not(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            set_bool_result(result, i, !get_bool_at(a, i));
        }
    }
    return result;
}

struct ArrowArray* arrow_xor(struct ArrowArray* a, struct ArrowArray* b) {
    if (!a || !b || a->length != b->length) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        bool val_a = is_valid_at(a, i) && get_bool_at(a, i);
        bool val_b = is_valid_at(b, i) && get_bool_at(b, i);
        set_bool_result(result, i, val_a != val_b);
    }
    return result;
}

// ============================================================================
// Aggregation Operations Implementation
// ============================================================================

AggregateResult arrow_min_int64(struct ArrowArray* a) {
    AggregateResult result = {false, 0, 0.0};
    if (!a || a->length == 0) return result;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            int64_t val = get_int64_at(a, i);
            if (!result.is_valid || val < result.i64_value) {
                result.is_valid = true;
                result.i64_value = val;
            }
        }
    }
    return result;
}

AggregateResult arrow_max_int64(struct ArrowArray* a) {
    AggregateResult result = {false, 0, 0.0};
    if (!a || a->length == 0) return result;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            int64_t val = get_int64_at(a, i);
            if (!result.is_valid || val > result.i64_value) {
                result.is_valid = true;
                result.i64_value = val;
            }
        }
    }
    return result;
}

AggregateResult arrow_min_float64(struct ArrowArray* a) {
    AggregateResult result = {false, 0, 0.0};
    if (!a || a->length == 0) return result;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            double val = get_float64_at(a, i);
            if (!result.is_valid || val < result.f64_value) {
                result.is_valid = true;
                result.f64_value = val;
            }
        }
    }
    return result;
}

AggregateResult arrow_max_float64(struct ArrowArray* a) {
    AggregateResult result = {false, 0, 0.0};
    if (!a || a->length == 0) return result;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            double val = get_float64_at(a, i);
            if (!result.is_valid || val > result.f64_value) {
                result.is_valid = true;
                result.f64_value = val;
            }
        }
    }
    return result;
}

AggregateResult arrow_sum_int64(struct ArrowArray* a) {
    AggregateResult result = {false, 0, 0.0};
    if (!a || a->length == 0) return result;

    int64_t sum = 0;
    bool has_value = false;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            sum += get_int64_at(a, i);
            has_value = true;
        }
    }

    if (has_value) {
        result.is_valid = true;
        result.i64_value = sum;
    }
    return result;
}

AggregateResult arrow_sum_float64(struct ArrowArray* a) {
    AggregateResult result = {false, 0, 0.0};
    if (!a || a->length == 0) return result;

    double sum = 0.0;
    bool has_value = false;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            sum += get_float64_at(a, i);
            has_value = true;
        }
    }

    if (has_value) {
        result.is_valid = true;
        result.f64_value = sum;
    }
    return result;
}

AggregateResult arrow_mean_int64(struct ArrowArray* a) {
    AggregateResult result = {false, 0, 0.0};
    if (!a || a->length == 0) return result;

    int64_t sum = 0;
    int64_t count = 0;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            sum += get_int64_at(a, i);
            count++;
        }
    }

    if (count > 0) {
        result.is_valid = true;
        result.f64_value = (double)sum / (double)count;
    }
    return result;
}

AggregateResult arrow_mean_float64(struct ArrowArray* a) {
    AggregateResult result = {false, 0, 0.0};
    if (!a || a->length == 0) return result;

    double sum = 0.0;
    int64_t count = 0;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            sum += get_float64_at(a, i);
            count++;
        }
    }

    if (count > 0) {
        result.is_valid = true;
        result.f64_value = sum / (double)count;
    }
    return result;
}

AggregateResult arrow_variance_float64(struct ArrowArray* a) {
    AggregateResult result = {false, 0, 0.0};
    if (!a || a->length == 0) return result;

    // Two-pass algorithm
    AggregateResult mean = arrow_mean_float64(a);
    if (!mean.is_valid) return result;

    double sum_sq_diff = 0.0;
    int64_t count = 0;

    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            double diff = get_float64_at(a, i) - mean.f64_value;
            sum_sq_diff += diff * diff;
            count++;
        }
    }

    if (count > 1) {
        result.is_valid = true;
        result.f64_value = sum_sq_diff / (double)(count - 1);  // Sample variance
    }
    return result;
}

AggregateResult arrow_stddev_float64(struct ArrowArray* a) {
    AggregateResult var = arrow_variance_float64(a);
    if (var.is_valid) {
        var.f64_value = sqrt(var.f64_value);
    }
    return var;
}

int64_t arrow_count(struct ArrowArray* a) {
    if (!a) return 0;
    return a->length - a->null_count;
}

int64_t arrow_count_all(struct ArrowArray* a) {
    if (!a) return 0;
    return a->length;
}

int64_t arrow_count_distinct_int64(struct ArrowArray* a) {
    if (!a || a->length == 0) return 0;

    // Simple O(n^2) algorithm for small arrays
    // For large arrays, use a hash set
    int64_t count = 0;
    for (int64_t i = 0; i < a->length; i++) {
        if (!is_valid_at(a, i)) continue;
        int64_t val = get_int64_at(a, i);
        bool seen = false;
        for (int64_t j = 0; j < i; j++) {
            if (is_valid_at(a, j) && get_int64_at(a, j) == val) {
                seen = true;
                break;
            }
        }
        if (!seen) count++;
    }
    return count;
}

int64_t arrow_count_distinct_string(struct ArrowArray* a) {
    if (!a || a->length == 0) return 0;

    int64_t count = 0;
    for (int64_t i = 0; i < a->length; i++) {
        if (!is_valid_at(a, i)) continue;
        int32_t len_i;
        const char* str_i = get_string_at(a, i, &len_i);
        bool seen = false;
        for (int64_t j = 0; j < i; j++) {
            if (!is_valid_at(a, j)) continue;
            int32_t len_j;
            const char* str_j = get_string_at(a, j, &len_j);
            if (len_i == len_j && memcmp(str_i, str_j, len_i) == 0) {
                seen = true;
                break;
            }
        }
        if (!seen) count++;
    }
    return count;
}

bool arrow_any(struct ArrowArray* a) {
    if (!a) return false;
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && get_bool_at(a, i)) {
            return true;
        }
    }
    return false;
}

bool arrow_all(struct ArrowArray* a) {
    if (!a || a->length == 0) return true;
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i) && !get_bool_at(a, i)) {
            return false;
        }
    }
    return true;
}

// ============================================================================
// String Operations Implementation
// ============================================================================

struct ArrowArray* arrow_string_length(struct ArrowArray* strings) {
    if (!strings) return NULL;

    struct ArrowArray* result = calloc(1, sizeof(struct ArrowArray));
    if (!result) return NULL;

    result->length = strings->length;
    result->null_count = strings->null_count;
    result->offset = 0;
    result->n_buffers = 2;
    result->n_children = 0;
    result->buffers = calloc(2, sizeof(void*));
    result->buffers[0] = NULL;  // Copy validity if needed
    result->buffers[1] = calloc(strings->length, sizeof(int32_t));
    result->children = NULL;
    result->dictionary = NULL;
    result->release = release_computed_array;

    int32_t* lengths = (int32_t*)result->buffers[1];
    for (int64_t i = 0; i < strings->length; i++) {
        if (is_valid_at(strings, i)) {
            int32_t len;
            get_string_at(strings, i, &len);
            lengths[i] = len;
        }
    }
    return result;
}

struct ArrowArray* arrow_string_contains(struct ArrowArray* strings, const char* pattern) {
    if (!strings || !pattern) return NULL;

    struct ArrowArray* result = create_bool_result(strings->length);
    if (!result) return NULL;

    size_t pattern_len = strlen(pattern);
    for (int64_t i = 0; i < strings->length; i++) {
        if (is_valid_at(strings, i)) {
            int32_t len;
            const char* str = get_string_at(strings, i, &len);
            // Simple substring search
            bool found = false;
            if (pattern_len <= (size_t)len) {
                for (int32_t j = 0; j <= len - (int32_t)pattern_len; j++) {
                    if (memcmp(str + j, pattern, pattern_len) == 0) {
                        found = true;
                        break;
                    }
                }
            }
            set_bool_result(result, i, found);
        }
    }
    return result;
}

struct ArrowArray* arrow_string_starts_with(struct ArrowArray* strings, const char* prefix) {
    if (!strings || !prefix) return NULL;

    struct ArrowArray* result = create_bool_result(strings->length);
    if (!result) return NULL;

    size_t prefix_len = strlen(prefix);
    for (int64_t i = 0; i < strings->length; i++) {
        if (is_valid_at(strings, i)) {
            int32_t len;
            const char* str = get_string_at(strings, i, &len);
            bool starts = (prefix_len <= (size_t)len) && (memcmp(str, prefix, prefix_len) == 0);
            set_bool_result(result, i, starts);
        }
    }
    return result;
}

struct ArrowArray* arrow_string_ends_with(struct ArrowArray* strings, const char* suffix) {
    if (!strings || !suffix) return NULL;

    struct ArrowArray* result = create_bool_result(strings->length);
    if (!result) return NULL;

    size_t suffix_len = strlen(suffix);
    for (int64_t i = 0; i < strings->length; i++) {
        if (is_valid_at(strings, i)) {
            int32_t len;
            const char* str = get_string_at(strings, i, &len);
            bool ends = (suffix_len <= (size_t)len) &&
                        (memcmp(str + len - suffix_len, suffix, suffix_len) == 0);
            set_bool_result(result, i, ends);
        }
    }
    return result;
}

// Placeholder implementations for remaining string operations
struct ArrowArray* arrow_substring(struct ArrowArray* strings, int32_t start, int32_t length) {
    (void)strings; (void)start; (void)length;
    // TODO: Implement
    return NULL;
}

struct ArrowArray* arrow_string_upper(struct ArrowArray* strings) {
    (void)strings;
    // TODO: Implement
    return NULL;
}

struct ArrowArray* arrow_string_lower(struct ArrowArray* strings) {
    (void)strings;
    // TODO: Implement
    return NULL;
}

struct ArrowArray* arrow_string_trim(struct ArrowArray* strings) {
    (void)strings;
    // TODO: Implement
    return NULL;
}

// ============================================================================
// Cast Operations Implementation (placeholders)
// ============================================================================

struct ArrowArray* arrow_cast_int64_to_int32(struct ArrowArray* a) {
    (void)a;
    return NULL;
}

struct ArrowArray* arrow_cast_int32_to_int64(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = (int64_t)get_int32_at(a, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_cast_float64_to_float32(struct ArrowArray* a) {
    (void)a;
    return NULL;
}

struct ArrowArray* arrow_cast_float32_to_float64(struct ArrowArray* a) {
    (void)a;
    return NULL;
}

struct ArrowArray* arrow_cast_int64_to_float64(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = (double)get_int64_at(a, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_cast_int32_to_float64(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = (double)get_int32_at(a, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_cast_float64_to_int64(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        if (is_valid_at(a, i)) {
            out[i] = (int64_t)get_float64_at(a, i);
        }
    }
    return result;
}

struct ArrowArray* arrow_cast_int64_to_string(struct ArrowArray* a) {
    (void)a;
    return NULL;
}

struct ArrowArray* arrow_cast_float64_to_string(struct ArrowArray* a) {
    (void)a;
    return NULL;
}

struct ArrowArray* arrow_cast_bool_to_string(struct ArrowArray* a) {
    (void)a;
    return NULL;
}

struct ArrowArray* arrow_cast_string_to_int64(struct ArrowArray* a) {
    (void)a;
    return NULL;
}

struct ArrowArray* arrow_cast_string_to_float64(struct ArrowArray* a) {
    (void)a;
    return NULL;
}

// ============================================================================
// Filter/Take/Sort Operations Implementation
// ============================================================================

struct ArrowArray* arrow_filter_int64(struct ArrowArray* values, struct ArrowArray* bool_mask) {
    if (!values || !bool_mask || values->length != bool_mask->length) return NULL;

    // Count true values
    int64_t count = 0;
    for (int64_t i = 0; i < bool_mask->length; i++) {
        if (is_valid_at(bool_mask, i) && get_bool_at(bool_mask, i)) {
            count++;
        }
    }

    struct ArrowArray* result = create_int64_result(count);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    int64_t out_idx = 0;
    for (int64_t i = 0; i < bool_mask->length; i++) {
        if (is_valid_at(bool_mask, i) && get_bool_at(bool_mask, i)) {
            out[out_idx++] = is_valid_at(values, i) ? get_int64_at(values, i) : 0;
        }
    }
    return result;
}

struct ArrowArray* arrow_filter_float64(struct ArrowArray* values, struct ArrowArray* bool_mask) {
    if (!values || !bool_mask || values->length != bool_mask->length) return NULL;

    int64_t count = 0;
    for (int64_t i = 0; i < bool_mask->length; i++) {
        if (is_valid_at(bool_mask, i) && get_bool_at(bool_mask, i)) {
            count++;
        }
    }

    struct ArrowArray* result = create_float64_result(count);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    int64_t out_idx = 0;
    for (int64_t i = 0; i < bool_mask->length; i++) {
        if (is_valid_at(bool_mask, i) && get_bool_at(bool_mask, i)) {
            out[out_idx++] = is_valid_at(values, i) ? get_float64_at(values, i) : 0.0;
        }
    }
    return result;
}

struct ArrowArray* arrow_filter_string(struct ArrowArray* values, struct ArrowArray* bool_mask) {
    (void)values; (void)bool_mask;
    // TODO: Implement
    return NULL;
}

struct ArrowArray* arrow_filter_bool(struct ArrowArray* values, struct ArrowArray* bool_mask) {
    if (!values || !bool_mask || values->length != bool_mask->length) return NULL;

    int64_t count = 0;
    for (int64_t i = 0; i < bool_mask->length; i++) {
        if (is_valid_at(bool_mask, i) && get_bool_at(bool_mask, i)) {
            count++;
        }
    }

    struct ArrowArray* result = create_bool_result(count);
    if (!result) return NULL;

    int64_t out_idx = 0;
    for (int64_t i = 0; i < bool_mask->length; i++) {
        if (is_valid_at(bool_mask, i) && get_bool_at(bool_mask, i)) {
            set_bool_result(result, out_idx++,
                            is_valid_at(values, i) ? get_bool_at(values, i) : false);
        }
    }
    return result;
}

struct ArrowArray* arrow_take_int64(struct ArrowArray* values, struct ArrowArray* int32_indices) {
    if (!values || !int32_indices) return NULL;

    struct ArrowArray* result = create_int64_result(int32_indices->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < int32_indices->length; i++) {
        if (is_valid_at(int32_indices, i)) {
            int32_t idx = get_int32_at(int32_indices, i);
            if (idx >= 0 && idx < values->length && is_valid_at(values, idx)) {
                out[i] = get_int64_at(values, idx);
            }
        }
    }
    return result;
}

struct ArrowArray* arrow_take_float64(struct ArrowArray* values, struct ArrowArray* int32_indices) {
    if (!values || !int32_indices) return NULL;

    struct ArrowArray* result = create_float64_result(int32_indices->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < int32_indices->length; i++) {
        if (is_valid_at(int32_indices, i)) {
            int32_t idx = get_int32_at(int32_indices, i);
            if (idx >= 0 && idx < values->length && is_valid_at(values, idx)) {
                out[i] = get_float64_at(values, idx);
            }
        }
    }
    return result;
}

struct ArrowArray* arrow_take_string(struct ArrowArray* values, struct ArrowArray* int32_indices) {
    (void)values; (void)int32_indices;
    // TODO: Implement
    return NULL;
}

// Helper for sorting - simple insertion sort for now
static void sort_indices_int64_impl(struct ArrowArray* values, int32_t* indices, int64_t n, bool ascending) {
    for (int64_t i = 1; i < n; i++) {
        int32_t key = indices[i];
        int64_t key_val = is_valid_at(values, key) ? get_int64_at(values, key) : 0;
        int64_t j = i - 1;

        while (j >= 0) {
            int64_t j_val = is_valid_at(values, indices[j]) ? get_int64_at(values, indices[j]) : 0;
            bool should_swap = ascending ? (j_val > key_val) : (j_val < key_val);
            if (!should_swap) break;
            indices[j + 1] = indices[j];
            j--;
        }
        indices[j + 1] = key;
    }
}

struct ArrowArray* arrow_sort_indices_int64(struct ArrowArray* values, bool ascending) {
    if (!values) return NULL;

    struct ArrowArray* result = calloc(1, sizeof(struct ArrowArray));
    if (!result) return NULL;

    result->length = values->length;
    result->null_count = 0;
    result->offset = 0;
    result->n_buffers = 2;
    result->n_children = 0;
    result->buffers = calloc(2, sizeof(void*));
    result->buffers[0] = NULL;
    result->buffers[1] = calloc(values->length, sizeof(int32_t));
    result->children = NULL;
    result->dictionary = NULL;
    result->release = release_computed_array;

    int32_t* indices = (int32_t*)result->buffers[1];
    for (int64_t i = 0; i < values->length; i++) {
        indices[i] = (int32_t)i;
    }

    sort_indices_int64_impl(values, indices, values->length, ascending);
    return result;
}

struct ArrowArray* arrow_sort_indices_float64(struct ArrowArray* values, bool ascending) {
    (void)values; (void)ascending;
    // TODO: Implement
    return NULL;
}

struct ArrowArray* arrow_sort_indices_string(struct ArrowArray* values, bool ascending) {
    (void)values; (void)ascending;
    // TODO: Implement
    return NULL;
}

// ============================================================================
// Null Handling Implementation
// ============================================================================

struct ArrowArray* arrow_is_null(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        set_bool_result(result, i, !is_valid_at(a, i));
    }
    return result;
}

struct ArrowArray* arrow_is_valid(struct ArrowArray* a) {
    if (!a) return NULL;

    struct ArrowArray* result = create_bool_result(a->length);
    if (!result) return NULL;

    for (int64_t i = 0; i < a->length; i++) {
        set_bool_result(result, i, is_valid_at(a, i));
    }
    return result;
}

struct ArrowArray* arrow_fill_null_int64(struct ArrowArray* a, int64_t fill_value) {
    if (!a) return NULL;

    struct ArrowArray* result = create_int64_result(a->length);
    if (!result) return NULL;

    int64_t* out = (int64_t*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        out[i] = is_valid_at(a, i) ? get_int64_at(a, i) : fill_value;
    }
    return result;
}

struct ArrowArray* arrow_fill_null_float64(struct ArrowArray* a, double fill_value) {
    if (!a) return NULL;

    struct ArrowArray* result = create_float64_result(a->length);
    if (!result) return NULL;

    double* out = (double*)result->buffers[1];
    for (int64_t i = 0; i < a->length; i++) {
        out[i] = is_valid_at(a, i) ? get_float64_at(a, i) : fill_value;
    }
    return result;
}

struct ArrowArray* arrow_fill_null_string(struct ArrowArray* a, const char* fill_value) {
    (void)a; (void)fill_value;
    // TODO: Implement
    return NULL;
}

struct ArrowArray* arrow_drop_null(struct ArrowArray* a) {
    (void)a;
    // TODO: Implement
    return NULL;
}

// ============================================================================
// Utility Functions
// ============================================================================

void arrow_compute_array_free(struct ArrowArray* array) {
    if (!array) return;
    if (array->release) {
        array->release(array);
    }
    free(array);
}

const char* arrow_compute_get_format(struct ArrowArray* array) {
    (void)array;
    // This would need schema info which arrays don't carry
    return NULL;
}
