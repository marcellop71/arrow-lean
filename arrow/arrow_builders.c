// Enable POSIX extensions for strdup
#define _POSIX_C_SOURCE 200809L

#include "arrow_builders.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// ============================================================================
// Private: Array Release Callbacks
// ============================================================================

// Private data for tracking builder-created arrays
typedef struct {
    void* buffer0;      // validity bitmap
    void* buffer1;      // values or offsets
    void* buffer2;      // data (for strings)
    void* builder;      // original builder (if kept for reference)
} ArrayPrivateData;

static void int64_array_release(struct ArrowArray* array) {
    if (array->release == NULL) return;

    ArrayPrivateData* priv = (ArrayPrivateData*)array->private_data;
    if (priv) {
        free(priv->buffer0);  // validity
        free(priv->buffer1);  // values
        free(priv);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }

    array->release = NULL;
}

static void float64_array_release(struct ArrowArray* array) {
    if (array->release == NULL) return;

    ArrayPrivateData* priv = (ArrayPrivateData*)array->private_data;
    if (priv) {
        free(priv->buffer0);
        free(priv->buffer1);
        free(priv);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }

    array->release = NULL;
}

static void string_array_release(struct ArrowArray* array) {
    if (array->release == NULL) return;

    ArrayPrivateData* priv = (ArrayPrivateData*)array->private_data;
    if (priv) {
        free(priv->buffer0);  // validity
        free(priv->buffer1);  // offsets
        free(priv->buffer2);  // data
        free(priv);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }

    array->release = NULL;
}

static void bool_array_release(struct ArrowArray* array) {
    if (array->release == NULL) return;

    ArrayPrivateData* priv = (ArrayPrivateData*)array->private_data;
    if (priv) {
        free(priv->buffer0);
        free(priv->buffer1);
        free(priv);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }

    array->release = NULL;
}

// Generic release for fixed-width types (int8, int16, int32, uint8, uint16, uint32, uint64, float32, date32, date64, time32, time64, duration)
static void fixed_width_array_release(struct ArrowArray* array) {
    if (array->release == NULL) return;

    ArrayPrivateData* priv = (ArrayPrivateData*)array->private_data;
    if (priv) {
        free(priv->buffer0);
        free(priv->buffer1);
        free(priv);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }

    array->release = NULL;
}

static void binary_array_release(struct ArrowArray* array) {
    if (array->release == NULL) return;

    ArrayPrivateData* priv = (ArrayPrivateData*)array->private_data;
    if (priv) {
        free(priv->buffer0);  // validity
        free(priv->buffer1);  // offsets
        free(priv->buffer2);  // data
        free(priv);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }

    array->release = NULL;
}

// ============================================================================
// Int64 Builder Implementation
// ============================================================================

Int64Builder* int64_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    Int64Builder* builder = calloc(1, sizeof(Int64Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(int64_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        int64_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int int64_builder_ensure_capacity(Int64Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int64_t* new_values = realloc(builder->values, new_capacity * sizeof(int64_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    // Zero out new bitmap bytes
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int int64_builder_append(Int64Builder* builder, int64_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = int64_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);  // Not null
    builder->length++;

    return BUILDER_OK;
}

int int64_builder_append_null(Int64Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = int64_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;  // Placeholder
    bitmap_set(builder->validity, builder->length, false);  // Is null
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

int int64_builder_append_values(Int64Builder* builder, const int64_t* values, size_t count) {
    if (!builder) return BUILDER_ERR_NULL;
    if (!values && count > 0) return BUILDER_ERR_NULL;

    int err = int64_builder_ensure_capacity(builder, count);
    if (err != BUILDER_OK) return err;

    memcpy(builder->values + builder->length, values, count * sizeof(int64_t));
    for (size_t i = 0; i < count; i++) {
        bitmap_set(builder->validity, builder->length + i, true);
    }
    builder->length += count;

    return BUILDER_OK;
}

struct ArrowArray* int64_builder_finish(Int64Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    // Transfer ownership of buffers
    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    // Create private data for release callback
    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = int64_array_release;

    // Clear builder (buffers now owned by array)
    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void int64_builder_reset(Int64Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void int64_builder_free(Int64Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t int64_builder_length(const Int64Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Float64 Builder Implementation
// ============================================================================

Float64Builder* float64_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    Float64Builder* builder = calloc(1, sizeof(Float64Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(double));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        float64_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int float64_builder_ensure_capacity(Float64Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    double* new_values = realloc(builder->values, new_capacity * sizeof(double));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int float64_builder_append(Float64Builder* builder, double value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = float64_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int float64_builder_append_null(Float64Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = float64_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0.0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

int float64_builder_append_values(Float64Builder* builder, const double* values, size_t count) {
    if (!builder) return BUILDER_ERR_NULL;
    if (!values && count > 0) return BUILDER_ERR_NULL;

    int err = float64_builder_ensure_capacity(builder, count);
    if (err != BUILDER_OK) return err;

    memcpy(builder->values + builder->length, values, count * sizeof(double));
    for (size_t i = 0; i < count; i++) {
        bitmap_set(builder->validity, builder->length + i, true);
    }
    builder->length += count;

    return BUILDER_OK;
}

struct ArrowArray* float64_builder_finish(Float64Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = float64_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void float64_builder_reset(Float64Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void float64_builder_free(Float64Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t float64_builder_length(const Float64Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// String Builder Implementation
// ============================================================================

StringBuilder* string_builder_create(size_t initial_capacity, size_t initial_data_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;
    if (initial_data_capacity == 0) initial_data_capacity = 8192;

    StringBuilder* builder = calloc(1, sizeof(StringBuilder));
    if (!builder) return NULL;

    // Offsets array has length+1 elements
    builder->offsets = malloc((initial_capacity + 1) * sizeof(int32_t));
    builder->data = malloc(initial_data_capacity);
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->data_capacity = initial_data_capacity;
    builder->length = 0;
    builder->data_length = 0;
    builder->null_count = 0;

    if (!builder->offsets || !builder->data || !builder->validity) {
        string_builder_free(builder);
        return NULL;
    }

    builder->offsets[0] = 0;  // First offset is always 0
    return builder;
}

static int string_builder_ensure_capacity(StringBuilder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int32_t* new_offsets = realloc(builder->offsets, (new_capacity + 1) * sizeof(int32_t));
    if (!new_offsets) return BUILDER_ERR_ALLOC;
    builder->offsets = new_offsets;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

static int string_builder_ensure_data_capacity(StringBuilder* builder, size_t additional) {
    size_t required = builder->data_length + additional;
    if (required <= builder->data_capacity) return BUILDER_OK;

    size_t new_capacity = builder->data_capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    char* new_data = realloc(builder->data, new_capacity);
    if (!new_data) return BUILDER_ERR_ALLOC;
    builder->data = new_data;
    builder->data_capacity = new_capacity;

    return BUILDER_OK;
}

int string_builder_append(StringBuilder* builder, const char* value, size_t length) {
    if (!builder) return BUILDER_ERR_NULL;
    if (!value && length > 0) return BUILDER_ERR_NULL;

    int err = string_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    err = string_builder_ensure_data_capacity(builder, length);
    if (err != BUILDER_OK) return err;

    // Copy string data
    if (length > 0) {
        memcpy(builder->data + builder->data_length, value, length);
    }
    builder->data_length += length;

    // Update offset for next string
    builder->offsets[builder->length + 1] = (int32_t)builder->data_length;

    // Mark as valid
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int string_builder_append_cstr(StringBuilder* builder, const char* value) {
    if (!value) return string_builder_append_null(builder);
    return string_builder_append(builder, value, strlen(value));
}

int string_builder_append_null(StringBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = string_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    // Offset stays the same (empty string for null)
    builder->offsets[builder->length + 1] = (int32_t)builder->data_length;

    // Mark as null
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* string_builder_finish(StringBuilder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 3;  // validity, offsets, data
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(3 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->offsets;
    array->buffers[2] = builder->data;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->offsets;
    priv->buffer2 = builder->data;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = string_array_release;

    builder->offsets = NULL;
    builder->data = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->data_length = 0;
    builder->data_capacity = 0;
    builder->null_count = 0;

    return array;
}

void string_builder_reset(StringBuilder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->data_length = 0;
    builder->null_count = 0;
    if (builder->offsets) builder->offsets[0] = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void string_builder_free(StringBuilder* builder) {
    if (!builder) return;
    free(builder->offsets);
    free(builder->data);
    free(builder->validity);
    free(builder);
}

size_t string_builder_length(const StringBuilder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Timestamp Builder Implementation
// ============================================================================

TimestampBuilder* timestamp_builder_create(size_t initial_capacity, const char* timezone) {
    if (initial_capacity == 0) initial_capacity = 1024;

    TimestampBuilder* builder = calloc(1, sizeof(TimestampBuilder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(int64_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (timezone) {
        builder->timezone = strdup(timezone);
    } else {
        builder->timezone = strdup("UTC");
    }

    if (!builder->values || !builder->validity || !builder->timezone) {
        timestamp_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int timestamp_builder_ensure_capacity(TimestampBuilder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int64_t* new_values = realloc(builder->values, new_capacity * sizeof(int64_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int timestamp_builder_append(TimestampBuilder* builder, int64_t microseconds) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = timestamp_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = microseconds;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int timestamp_builder_append_null(TimestampBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = timestamp_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* timestamp_builder_finish(TimestampBuilder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = int64_array_release;  // Same release as int64

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;
    // Note: timezone is NOT freed - builder still owns it

    return array;
}

void timestamp_builder_reset(TimestampBuilder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void timestamp_builder_free(TimestampBuilder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder->timezone);
    free(builder);
}

size_t timestamp_builder_length(const TimestampBuilder* builder) {
    return builder ? builder->length : 0;
}

const char* timestamp_builder_get_timezone(const TimestampBuilder* builder) {
    return builder ? builder->timezone : NULL;
}

// ============================================================================
// Bool Builder Implementation
// ============================================================================

BoolBuilder* bool_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    BoolBuilder* builder = calloc(1, sizeof(BoolBuilder));
    if (!builder) return NULL;

    builder->values = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        bool_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int bool_builder_ensure_capacity(BoolBuilder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);

    uint8_t* new_values = realloc(builder->values, new_bitmap_size);
    if (!new_values) return BUILDER_ERR_ALLOC;
    memset(new_values + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->values = new_values;

    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int bool_builder_append(BoolBuilder* builder, bool value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = bool_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    bitmap_set(builder->values, builder->length, value);
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int bool_builder_append_null(BoolBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = bool_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    bitmap_set(builder->values, builder->length, false);
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* bool_builder_finish(BoolBuilder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = bool_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void bool_builder_reset(BoolBuilder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->values) {
        memset(builder->values, 0, bitmap_byte_count(builder->capacity));
    }
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void bool_builder_free(BoolBuilder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t bool_builder_length(const BoolBuilder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Int8 Builder Implementation
// ============================================================================

Int8Builder* int8_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    Int8Builder* builder = calloc(1, sizeof(Int8Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(int8_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        int8_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int int8_builder_ensure_capacity(Int8Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int8_t* new_values = realloc(builder->values, new_capacity * sizeof(int8_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int int8_builder_append(Int8Builder* builder, int8_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = int8_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int int8_builder_append_null(Int8Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = int8_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* int8_builder_finish(Int8Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void int8_builder_reset(Int8Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void int8_builder_free(Int8Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t int8_builder_length(const Int8Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Int16 Builder Implementation
// ============================================================================

Int16Builder* int16_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    Int16Builder* builder = calloc(1, sizeof(Int16Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(int16_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        int16_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int int16_builder_ensure_capacity(Int16Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int16_t* new_values = realloc(builder->values, new_capacity * sizeof(int16_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int int16_builder_append(Int16Builder* builder, int16_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = int16_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int int16_builder_append_null(Int16Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = int16_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* int16_builder_finish(Int16Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void int16_builder_reset(Int16Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void int16_builder_free(Int16Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t int16_builder_length(const Int16Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Int32 Builder Implementation
// ============================================================================

Int32Builder* int32_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    Int32Builder* builder = calloc(1, sizeof(Int32Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(int32_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        int32_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int int32_builder_ensure_capacity(Int32Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int32_t* new_values = realloc(builder->values, new_capacity * sizeof(int32_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int int32_builder_append(Int32Builder* builder, int32_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = int32_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int int32_builder_append_null(Int32Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = int32_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* int32_builder_finish(Int32Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void int32_builder_reset(Int32Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void int32_builder_free(Int32Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t int32_builder_length(const Int32Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// UInt8 Builder Implementation
// ============================================================================

UInt8Builder* uint8_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    UInt8Builder* builder = calloc(1, sizeof(UInt8Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(uint8_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        uint8_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int uint8_builder_ensure_capacity(UInt8Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    uint8_t* new_values = realloc(builder->values, new_capacity * sizeof(uint8_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int uint8_builder_append(UInt8Builder* builder, uint8_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = uint8_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int uint8_builder_append_null(UInt8Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = uint8_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* uint8_builder_finish(UInt8Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void uint8_builder_reset(UInt8Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void uint8_builder_free(UInt8Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t uint8_builder_length(const UInt8Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// UInt16 Builder Implementation
// ============================================================================

UInt16Builder* uint16_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    UInt16Builder* builder = calloc(1, sizeof(UInt16Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(uint16_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        uint16_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int uint16_builder_ensure_capacity(UInt16Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    uint16_t* new_values = realloc(builder->values, new_capacity * sizeof(uint16_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int uint16_builder_append(UInt16Builder* builder, uint16_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = uint16_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int uint16_builder_append_null(UInt16Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = uint16_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* uint16_builder_finish(UInt16Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void uint16_builder_reset(UInt16Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void uint16_builder_free(UInt16Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t uint16_builder_length(const UInt16Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// UInt32 Builder Implementation
// ============================================================================

UInt32Builder* uint32_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    UInt32Builder* builder = calloc(1, sizeof(UInt32Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(uint32_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        uint32_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int uint32_builder_ensure_capacity(UInt32Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    uint32_t* new_values = realloc(builder->values, new_capacity * sizeof(uint32_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int uint32_builder_append(UInt32Builder* builder, uint32_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = uint32_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int uint32_builder_append_null(UInt32Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = uint32_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* uint32_builder_finish(UInt32Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void uint32_builder_reset(UInt32Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void uint32_builder_free(UInt32Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t uint32_builder_length(const UInt32Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// UInt64 Builder Implementation
// ============================================================================

UInt64Builder* uint64_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    UInt64Builder* builder = calloc(1, sizeof(UInt64Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(uint64_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        uint64_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int uint64_builder_ensure_capacity(UInt64Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    uint64_t* new_values = realloc(builder->values, new_capacity * sizeof(uint64_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int uint64_builder_append(UInt64Builder* builder, uint64_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = uint64_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int uint64_builder_append_null(UInt64Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = uint64_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* uint64_builder_finish(UInt64Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void uint64_builder_reset(UInt64Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void uint64_builder_free(UInt64Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t uint64_builder_length(const UInt64Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Float32 Builder Implementation
// ============================================================================

Float32Builder* float32_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    Float32Builder* builder = calloc(1, sizeof(Float32Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(float));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        float32_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int float32_builder_ensure_capacity(Float32Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    float* new_values = realloc(builder->values, new_capacity * sizeof(float));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int float32_builder_append(Float32Builder* builder, float value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = float32_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int float32_builder_append_null(Float32Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = float32_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0.0f;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* float32_builder_finish(Float32Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void float32_builder_reset(Float32Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void float32_builder_free(Float32Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t float32_builder_length(const Float32Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Date32 Builder Implementation
// ============================================================================

Date32Builder* date32_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    Date32Builder* builder = calloc(1, sizeof(Date32Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(int32_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        date32_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int date32_builder_ensure_capacity(Date32Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int32_t* new_values = realloc(builder->values, new_capacity * sizeof(int32_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int date32_builder_append(Date32Builder* builder, int32_t days) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = date32_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = days;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int date32_builder_append_null(Date32Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = date32_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* date32_builder_finish(Date32Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void date32_builder_reset(Date32Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void date32_builder_free(Date32Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t date32_builder_length(const Date32Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Date64 Builder Implementation
// ============================================================================

Date64Builder* date64_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;

    Date64Builder* builder = calloc(1, sizeof(Date64Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(int64_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;

    if (!builder->values || !builder->validity) {
        date64_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int date64_builder_ensure_capacity(Date64Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int64_t* new_values = realloc(builder->values, new_capacity * sizeof(int64_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int date64_builder_append(Date64Builder* builder, int64_t milliseconds) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = date64_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = milliseconds;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int date64_builder_append_null(Date64Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = date64_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* date64_builder_finish(Date64Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void date64_builder_reset(Date64Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void date64_builder_free(Date64Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t date64_builder_length(const Date64Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Time32 Builder Implementation
// ============================================================================

Time32Builder* time32_builder_create(size_t initial_capacity, char unit) {
    if (initial_capacity == 0) initial_capacity = 1024;
    if (unit != 's' && unit != 'm') unit = 's';  // Default to seconds

    Time32Builder* builder = calloc(1, sizeof(Time32Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(int32_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;
    builder->unit = unit;

    if (!builder->values || !builder->validity) {
        time32_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int time32_builder_ensure_capacity(Time32Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int32_t* new_values = realloc(builder->values, new_capacity * sizeof(int32_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int time32_builder_append(Time32Builder* builder, int32_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = time32_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int time32_builder_append_null(Time32Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = time32_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* time32_builder_finish(Time32Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void time32_builder_reset(Time32Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void time32_builder_free(Time32Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t time32_builder_length(const Time32Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Time64 Builder Implementation
// ============================================================================

Time64Builder* time64_builder_create(size_t initial_capacity, char unit) {
    if (initial_capacity == 0) initial_capacity = 1024;
    if (unit != 'u' && unit != 'n') unit = 'u';  // Default to microseconds

    Time64Builder* builder = calloc(1, sizeof(Time64Builder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(int64_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;
    builder->unit = unit;

    if (!builder->values || !builder->validity) {
        time64_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int time64_builder_ensure_capacity(Time64Builder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int64_t* new_values = realloc(builder->values, new_capacity * sizeof(int64_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int time64_builder_append(Time64Builder* builder, int64_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = time64_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int time64_builder_append_null(Time64Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = time64_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* time64_builder_finish(Time64Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void time64_builder_reset(Time64Builder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void time64_builder_free(Time64Builder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t time64_builder_length(const Time64Builder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Duration Builder Implementation
// ============================================================================

DurationBuilder* duration_builder_create(size_t initial_capacity, char unit) {
    if (initial_capacity == 0) initial_capacity = 1024;
    if (unit != 's' && unit != 'm' && unit != 'u' && unit != 'n') unit = 'u';  // Default to microseconds

    DurationBuilder* builder = calloc(1, sizeof(DurationBuilder));
    if (!builder) return NULL;

    builder->values = malloc(initial_capacity * sizeof(int64_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->length = 0;
    builder->null_count = 0;
    builder->unit = unit;

    if (!builder->values || !builder->validity) {
        duration_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int duration_builder_ensure_capacity(DurationBuilder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int64_t* new_values = realloc(builder->values, new_capacity * sizeof(int64_t));
    if (!new_values) return BUILDER_ERR_ALLOC;
    builder->values = new_values;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int duration_builder_append(DurationBuilder* builder, int64_t value) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = duration_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = value;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int duration_builder_append_null(DurationBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = duration_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->values[builder->length] = 0;
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* duration_builder_finish(DurationBuilder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(2 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->values;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->values;
    priv->buffer2 = NULL;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = fixed_width_array_release;

    builder->values = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->null_count = 0;

    return array;
}

void duration_builder_reset(DurationBuilder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->null_count = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void duration_builder_free(DurationBuilder* builder) {
    if (!builder) return;
    free(builder->values);
    free(builder->validity);
    free(builder);
}

size_t duration_builder_length(const DurationBuilder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Binary Builder Implementation
// ============================================================================

BinaryBuilder* binary_builder_create(size_t initial_capacity, size_t initial_data_capacity) {
    if (initial_capacity == 0) initial_capacity = 1024;
    if (initial_data_capacity == 0) initial_data_capacity = 8192;

    BinaryBuilder* builder = calloc(1, sizeof(BinaryBuilder));
    if (!builder) return NULL;

    builder->offsets = malloc((initial_capacity + 1) * sizeof(int32_t));
    builder->data = malloc(initial_data_capacity);
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->capacity = initial_capacity;
    builder->data_capacity = initial_data_capacity;
    builder->length = 0;
    builder->data_length = 0;
    builder->null_count = 0;

    if (!builder->offsets || !builder->data || !builder->validity) {
        binary_builder_free(builder);
        return NULL;
    }

    builder->offsets[0] = 0;
    return builder;
}

static int binary_builder_ensure_capacity(BinaryBuilder* builder, size_t additional) {
    size_t required = builder->length + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    int32_t* new_offsets = realloc(builder->offsets, (new_capacity + 1) * sizeof(int32_t));
    if (!new_offsets) return BUILDER_ERR_ALLOC;
    builder->offsets = new_offsets;

    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

static int binary_builder_ensure_data_capacity(BinaryBuilder* builder, size_t additional) {
    size_t required = builder->data_length + additional;
    if (required <= builder->data_capacity) return BUILDER_OK;

    size_t new_capacity = builder->data_capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    uint8_t* new_data = realloc(builder->data, new_capacity);
    if (!new_data) return BUILDER_ERR_ALLOC;
    builder->data = new_data;
    builder->data_capacity = new_capacity;

    return BUILDER_OK;
}

int binary_builder_append(BinaryBuilder* builder, const uint8_t* value, size_t length) {
    if (!builder) return BUILDER_ERR_NULL;
    if (!value && length > 0) return BUILDER_ERR_NULL;

    int err = binary_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    err = binary_builder_ensure_data_capacity(builder, length);
    if (err != BUILDER_OK) return err;

    if (length > 0) {
        memcpy(builder->data + builder->data_length, value, length);
    }
    builder->data_length += length;

    builder->offsets[builder->length + 1] = (int32_t)builder->data_length;

    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int binary_builder_append_null(BinaryBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int err = binary_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->offsets[builder->length + 1] = (int32_t)builder->data_length;

    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* binary_builder_finish(BinaryBuilder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 3;
    array->n_children = 0;
    array->children = NULL;
    array->dictionary = NULL;

    array->buffers = malloc(3 * sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }

    array->buffers[0] = builder->validity;
    array->buffers[1] = builder->offsets;
    array->buffers[2] = builder->data;

    ArrayPrivateData* priv = malloc(sizeof(ArrayPrivateData));
    if (!priv) {
        free((void*)array->buffers);
        free(array);
        return NULL;
    }
    priv->buffer0 = builder->validity;
    priv->buffer1 = builder->offsets;
    priv->buffer2 = builder->data;
    priv->builder = NULL;

    array->private_data = priv;
    array->release = binary_array_release;

    builder->offsets = NULL;
    builder->data = NULL;
    builder->validity = NULL;
    builder->length = 0;
    builder->capacity = 0;
    builder->data_length = 0;
    builder->data_capacity = 0;
    builder->null_count = 0;

    return array;
}

void binary_builder_reset(BinaryBuilder* builder) {
    if (!builder) return;
    builder->length = 0;
    builder->data_length = 0;
    builder->null_count = 0;
    if (builder->offsets) builder->offsets[0] = 0;
    if (builder->validity) {
        memset(builder->validity, 0, bitmap_byte_count(builder->capacity));
    }
}

void binary_builder_free(BinaryBuilder* builder) {
    if (!builder) return;
    free(builder->offsets);
    free(builder->data);
    free(builder->validity);
    free(builder);
}

size_t binary_builder_length(const BinaryBuilder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Schema Builder Implementation
// ============================================================================

SchemaBuilder* schema_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 16;

    SchemaBuilder* builder = calloc(1, sizeof(SchemaBuilder));
    if (!builder) return NULL;

    builder->names = calloc(initial_capacity, sizeof(char*));
    builder->formats = calloc(initial_capacity, sizeof(char*));
    builder->flags = calloc(initial_capacity, sizeof(int64_t));
    builder->capacity = initial_capacity;
    builder->count = 0;

    if (!builder->names || !builder->formats || !builder->flags) {
        schema_builder_free(builder);
        return NULL;
    }

    return builder;
}

static int schema_builder_ensure_capacity(SchemaBuilder* builder, size_t additional) {
    size_t required = builder->count + additional;
    if (required <= builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    char** new_names = realloc(builder->names, new_capacity * sizeof(char*));
    if (!new_names) return BUILDER_ERR_ALLOC;
    builder->names = new_names;

    char** new_formats = realloc(builder->formats, new_capacity * sizeof(char*));
    if (!new_formats) return BUILDER_ERR_ALLOC;
    builder->formats = new_formats;

    int64_t* new_flags = realloc(builder->flags, new_capacity * sizeof(int64_t));
    if (!new_flags) return BUILDER_ERR_ALLOC;
    builder->flags = new_flags;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int schema_builder_add_field(SchemaBuilder* builder, const char* name, const char* format, int64_t flags) {
    if (!builder || !name || !format) return BUILDER_ERR_NULL;

    int err = schema_builder_ensure_capacity(builder, 1);
    if (err != BUILDER_OK) return err;

    builder->names[builder->count] = strdup(name);
    builder->formats[builder->count] = strdup(format);
    builder->flags[builder->count] = flags;

    if (!builder->names[builder->count] || !builder->formats[builder->count]) {
        free(builder->names[builder->count]);
        free(builder->formats[builder->count]);
        return BUILDER_ERR_ALLOC;
    }

    builder->count++;
    return BUILDER_OK;
}

// Arrow format strings:
// "l" = int64
// "g" = float64
// "u" = utf8 string
// "b" = boolean
// "tsu:timezone" = timestamp microseconds with timezone

int schema_builder_add_int64(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "l", flags);
}

int schema_builder_add_float64(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "g", flags);
}

int schema_builder_add_string(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "u", flags);
}

int schema_builder_add_timestamp_us(SchemaBuilder* builder, const char* name, const char* timezone, bool nullable) {
    if (!timezone) timezone = "UTC";

    // Format: "tsu:timezone"
    size_t format_len = strlen("tsu:") + strlen(timezone) + 1;
    char* format = malloc(format_len);
    if (!format) return BUILDER_ERR_ALLOC;

    snprintf(format, format_len, "tsu:%s", timezone);

    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    int result = schema_builder_add_field(builder, name, format, flags);
    free(format);

    return result;
}

int schema_builder_add_bool(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "b", flags);
}

int schema_builder_add_int8(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "c", flags);
}

int schema_builder_add_int16(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "s", flags);
}

int schema_builder_add_int32(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "i", flags);
}

int schema_builder_add_uint8(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "C", flags);
}

int schema_builder_add_uint16(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "S", flags);
}

int schema_builder_add_uint32(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "I", flags);
}

int schema_builder_add_uint64(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "L", flags);
}

int schema_builder_add_float32(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "f", flags);
}

int schema_builder_add_date32(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "tdD", flags);
}

int schema_builder_add_date64(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "tdm", flags);
}

int schema_builder_add_time32(SchemaBuilder* builder, const char* name, char unit, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    // 's' for seconds, 'm' for milliseconds
    const char* format = (unit == 's') ? "tts" : "ttm";
    return schema_builder_add_field(builder, name, format, flags);
}

int schema_builder_add_time64(SchemaBuilder* builder, const char* name, char unit, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    // 'u' for microseconds, 'n' for nanoseconds
    const char* format = (unit == 'u') ? "ttu" : "ttn";
    return schema_builder_add_field(builder, name, format, flags);
}

int schema_builder_add_duration(SchemaBuilder* builder, const char* name, char unit, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    const char* format;
    switch (unit) {
        case 's': format = "tDs"; break;
        case 'm': format = "tDm"; break;
        case 'u': format = "tDu"; break;
        case 'n': format = "tDn"; break;
        default: format = "tDu"; break;  // Default to microseconds
    }
    return schema_builder_add_field(builder, name, format, flags);
}

int schema_builder_add_binary(SchemaBuilder* builder, const char* name, bool nullable) {
    int64_t flags = nullable ? ARROW_FLAG_NULLABLE : 0;
    return schema_builder_add_field(builder, name, "z", flags);
}

// Release callback for child schemas
static void child_schema_release(struct ArrowSchema* schema) {
    if (!schema || !schema->release) return;

    free((void*)schema->format);
    free((void*)schema->name);

    // Release children recursively
    if (schema->children) {
        for (int64_t i = 0; i < schema->n_children; i++) {
            if (schema->children[i] && schema->children[i]->release) {
                schema->children[i]->release(schema->children[i]);
            }
            free(schema->children[i]);
        }
        free(schema->children);
    }

    schema->release = NULL;
}

// Release callback for struct schema
static void struct_schema_release(struct ArrowSchema* schema) {
    if (!schema || !schema->release) return;

    free((void*)schema->format);
    free((void*)schema->name);

    if (schema->children) {
        for (int64_t i = 0; i < schema->n_children; i++) {
            if (schema->children[i] && schema->children[i]->release) {
                schema->children[i]->release(schema->children[i]);
            }
            free(schema->children[i]);
        }
        free(schema->children);
    }

    schema->release = NULL;
}

struct ArrowSchema* schema_builder_finish(SchemaBuilder* builder) {
    if (!builder) return NULL;

    // Create struct schema (root schema is always a struct for record batches)
    struct ArrowSchema* schema = calloc(1, sizeof(struct ArrowSchema));
    if (!schema) return NULL;

    schema->format = strdup("+s");  // struct format
    schema->name = NULL;
    schema->metadata = NULL;
    schema->flags = 0;
    schema->n_children = (int64_t)builder->count;
    schema->dictionary = NULL;

    if (builder->count > 0) {
        schema->children = calloc(builder->count, sizeof(struct ArrowSchema*));
        if (!schema->children) {
            free((void*)schema->format);
            free(schema);
            return NULL;
        }

        // Create child schemas
        for (size_t i = 0; i < builder->count; i++) {
            struct ArrowSchema* child = calloc(1, sizeof(struct ArrowSchema));
            if (!child) {
                // Cleanup on failure
                for (size_t j = 0; j < i; j++) {
                    if (schema->children[j]->release) {
                        schema->children[j]->release(schema->children[j]);
                    }
                    free(schema->children[j]);
                }
                free(schema->children);
                free((void*)schema->format);
                free(schema);
                return NULL;
            }

            child->format = strdup(builder->formats[i]);
            child->name = strdup(builder->names[i]);
            child->metadata = NULL;
            child->flags = builder->flags[i];
            child->n_children = 0;
            child->children = NULL;
            child->dictionary = NULL;
            child->release = child_schema_release;

            schema->children[i] = child;
        }
    } else {
        schema->children = NULL;
    }

    schema->release = struct_schema_release;

    return schema;
}

void schema_builder_reset(SchemaBuilder* builder) {
    if (!builder) return;

    for (size_t i = 0; i < builder->count; i++) {
        free(builder->names[i]);
        free(builder->formats[i]);
    }
    builder->count = 0;
}

void schema_builder_free(SchemaBuilder* builder) {
    if (!builder) return;

    for (size_t i = 0; i < builder->count; i++) {
        free(builder->names[i]);
        free(builder->formats[i]);
    }

    free(builder->names);
    free(builder->formats);
    free(builder->flags);
    free(builder);
}

size_t schema_builder_field_count(const SchemaBuilder* builder) {
    return builder ? builder->count : 0;
}

// ============================================================================
// RecordBatch Implementation
// ============================================================================

RecordBatch* record_batch_create(
    struct ArrowSchema* schema,
    struct ArrowArray** columns,
    size_t num_columns,
    size_t num_rows
) {
    if (!schema || (!columns && num_columns > 0)) return NULL;

    RecordBatch* batch = calloc(1, sizeof(RecordBatch));
    if (!batch) return NULL;

    batch->schema = schema;
    batch->columns = columns;
    batch->num_columns = num_columns;
    batch->num_rows = num_rows;

    return batch;
}

struct ArrowSchema* record_batch_get_schema(const RecordBatch* batch) {
    return batch ? batch->schema : NULL;
}

struct ArrowArray* record_batch_get_column(const RecordBatch* batch, size_t index) {
    if (!batch || index >= batch->num_columns) return NULL;
    return batch->columns[index];
}

size_t record_batch_num_rows(const RecordBatch* batch) {
    return batch ? batch->num_rows : 0;
}

size_t record_batch_num_columns(const RecordBatch* batch) {
    return batch ? batch->num_columns : 0;
}

// Release callback for struct array (record batch converted to array)
static void struct_array_release(struct ArrowArray* array) {
    if (!array || !array->release) return;

    // Release children
    if (array->children) {
        for (int64_t i = 0; i < array->n_children; i++) {
            if (array->children[i] && array->children[i]->release) {
                array->children[i]->release(array->children[i]);
            }
            free(array->children[i]);
        }
        free(array->children);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }

    // Free the RecordBatch stored in private_data (but not its columns, they're children)
    RecordBatch* batch = (RecordBatch*)array->private_data;
    if (batch) {
        // Don't free columns here - they're the children we already released
        // Don't free schema - it should be managed separately
        free(batch->columns);  // Just free the array of pointers
        free(batch);
    }

    array->release = NULL;
}

struct ArrowArray* record_batch_to_struct_array(RecordBatch* batch) {
    if (!batch) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    array->length = (int64_t)batch->num_rows;
    array->null_count = 0;
    array->offset = 0;
    array->n_buffers = 1;  // Just validity buffer (NULL for struct)
    array->n_children = (int64_t)batch->num_columns;
    array->dictionary = NULL;

    // Buffers - just one NULL buffer for struct (no validity bitmap needed for root)
    array->buffers = calloc(1, sizeof(void*));
    if (!array->buffers) {
        free(array);
        return NULL;
    }
    array->buffers[0] = NULL;  // No validity bitmap

    // Children are the columns
    if (batch->num_columns > 0) {
        array->children = calloc(batch->num_columns, sizeof(struct ArrowArray*));
        if (!array->children) {
            free((void*)array->buffers);
            free(array);
            return NULL;
        }

        for (size_t i = 0; i < batch->num_columns; i++) {
            array->children[i] = batch->columns[i];
        }
    } else {
        array->children = NULL;
    }

    array->private_data = batch;
    array->release = struct_array_release;

    return array;
}

void record_batch_free(RecordBatch* batch) {
    if (!batch) return;

    // Release schema
    if (batch->schema && batch->schema->release) {
        batch->schema->release(batch->schema);
    }
    free(batch->schema);

    // Release columns
    if (batch->columns) {
        for (size_t i = 0; i < batch->num_columns; i++) {
            if (batch->columns[i] && batch->columns[i]->release) {
                batch->columns[i]->release(batch->columns[i]);
            }
            free(batch->columns[i]);
        }
        free(batch->columns);
    }

    free(batch);
}

// ============================================================================
// Stream from Batches
// ============================================================================

// Private data for batch stream
typedef struct {
    struct ArrowSchema* schema;
    RecordBatch** batches;
    size_t num_batches;
    size_t current_batch;
    bool schema_consumed;
} BatchStreamPrivate;

static int batch_stream_get_schema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
    if (!stream || !out) return -1;

    BatchStreamPrivate* priv = (BatchStreamPrivate*)stream->private_data;
    if (!priv || !priv->schema) return -1;

    // Copy schema to output
    memcpy(out, priv->schema, sizeof(struct ArrowSchema));
    priv->schema_consumed = true;

    return 0;
}

static int batch_stream_get_next(struct ArrowArrayStream* stream, struct ArrowArray* out) {
    if (!stream || !out) return -1;

    BatchStreamPrivate* priv = (BatchStreamPrivate*)stream->private_data;
    if (!priv) return -1;

    // Initialize output
    memset(out, 0, sizeof(struct ArrowArray));

    if (priv->current_batch >= priv->num_batches) {
        // No more batches - return empty array to signal end
        out->release = NULL;
        return 0;
    }

    // Convert batch to struct array
    RecordBatch* batch = priv->batches[priv->current_batch];
    struct ArrowArray* arr = record_batch_to_struct_array(batch);
    if (!arr) return -1;

    // Copy to output
    memcpy(out, arr, sizeof(struct ArrowArray));
    free(arr);  // Free the ArrowArray struct itself, not its contents

    priv->current_batch++;
    return 0;
}

static const char* batch_stream_get_last_error(struct ArrowArrayStream* stream) {
    (void)stream;
    return NULL;
}

static void batch_stream_release(struct ArrowArrayStream* stream) {
    if (!stream || !stream->release) return;

    BatchStreamPrivate* priv = (BatchStreamPrivate*)stream->private_data;
    if (priv) {
        // Schema is shared, only free if not consumed
        if (!priv->schema_consumed && priv->schema) {
            if (priv->schema->release) {
                priv->schema->release(priv->schema);
            }
            free(priv->schema);
        }

        // Note: batches have been converted to arrays and ownership transferred
        // The remaining batches that weren't consumed should still be freed
        for (size_t i = priv->current_batch; i < priv->num_batches; i++) {
            record_batch_free(priv->batches[i]);
        }
        free(priv->batches);
        free(priv);
    }

    stream->release = NULL;
}

struct ArrowArrayStream* batch_to_stream(RecordBatch* batch) {
    if (!batch) return NULL;

    RecordBatch** batches = malloc(sizeof(RecordBatch*));
    if (!batches) return NULL;
    batches[0] = batch;

    return batches_to_stream(batch->schema, batches, 1);
}

struct ArrowArrayStream* batches_to_stream(
    struct ArrowSchema* schema,
    RecordBatch** batches,
    size_t num_batches
) {
    if (!schema) return NULL;

    struct ArrowArrayStream* stream = calloc(1, sizeof(struct ArrowArrayStream));
    if (!stream) return NULL;

    BatchStreamPrivate* priv = calloc(1, sizeof(BatchStreamPrivate));
    if (!priv) {
        free(stream);
        return NULL;
    }

    priv->schema = schema;
    priv->batches = batches;
    priv->num_batches = num_batches;
    priv->current_batch = 0;
    priv->schema_consumed = false;

    stream->get_schema = batch_stream_get_schema;
    stream->get_next = batch_stream_get_next;
    stream->get_last_error = batch_stream_get_last_error;
    stream->release = batch_stream_release;
    stream->private_data = priv;

    return stream;
}
