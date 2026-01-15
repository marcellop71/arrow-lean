#include "arrow_c_abi.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

// Helper function to check if a value is null
static bool is_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return true;
    
    // If null_count is 0, no nulls exist
    if (array->null_count == 0) return false;
    
    // Check validity buffer (first buffer)
    if (array->n_buffers > 0 && array->buffers[0] != NULL) {
        const uint8_t* validity = (const uint8_t*)array->buffers[0];
        size_t byte_index = index / 8;
        size_t bit_index = index % 8;
        return !(validity[byte_index] & (1 << bit_index));
    }
    
    return false;
}

bool arrow_get_bool_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return false; // Return false for null/invalid values
    }
    
    // Boolean values are stored in the second buffer (index 1)
    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return false;
    }
    
    const uint8_t* data = (const uint8_t*)array->buffers[1];
    size_t byte_index = (index + array->offset) / 8;
    size_t bit_index = (index + array->offset) % 8;
    
    return (data[byte_index] & (1 << bit_index)) != 0;
}

int64_t arrow_get_int64_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return 0; // Return 0 for null/invalid values
    }
    
    // Int64 values are stored in the second buffer (index 1)
    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return 0;
    }
    
    const int64_t* data = (const int64_t*)array->buffers[1];
    return data[index + array->offset];
}

const char* arrow_get_string_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return NULL;
    }
    
    // String arrays have 3 buffers: validity, offsets, and data
    if (array->n_buffers < 3 || array->buffers[1] == NULL || array->buffers[2] == NULL) {
        return NULL;
    }
    
    const int32_t* offsets = (const int32_t*)array->buffers[1];
    const char* data = (const char*)array->buffers[2];
    
    size_t actual_index = index + array->offset;
    int32_t start_offset = offsets[actual_index];
    int32_t end_offset = offsets[actual_index + 1];
    
    // Allocate memory for the string (caller needs to free this)
    size_t length = end_offset - start_offset;
    char* result = (char*)malloc(length + 1);
    if (!result) return NULL;
    
    memcpy(result, data + start_offset, length);
    result[length] = '\0';
    
    return result;
}

// Helper functions to check for null values - these return 1 if null, 0 if not null, -1 on error
int arrow_is_bool_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

int arrow_is_int64_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

int arrow_is_string_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

// Float64 data access
double arrow_get_float64_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return 0.0; // Return 0 for null/invalid values
    }

    // Float64 values are stored in the second buffer (index 1)
    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return 0.0;
    }

    const double* data = (const double*)array->buffers[1];
    return data[index + array->offset];
}

int arrow_is_float64_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

// UInt64 data access
uint64_t arrow_get_uint64_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return 0; // Return 0 for null/invalid values
    }

    // UInt64 values are stored in the second buffer (index 1)
    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return 0;
    }

    const uint64_t* data = (const uint64_t*)array->buffers[1];
    return data[index + array->offset];
}

int arrow_is_uint64_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

// Float32 data access
float arrow_get_float32_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return 0.0f;
    }

    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return 0.0f;
    }

    const float* data = (const float*)array->buffers[1];
    return data[index + array->offset];
}

int arrow_is_float32_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

// Int32 data access
int32_t arrow_get_int32_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return 0;
    }

    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return 0;
    }

    const int32_t* data = (const int32_t*)array->buffers[1];
    return data[index + array->offset];
}

int arrow_is_int32_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

// Int8 data access
int8_t arrow_get_int8_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return 0;
    }

    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return 0;
    }

    const int8_t* data = (const int8_t*)array->buffers[1];
    return data[index + array->offset];
}

int arrow_is_int8_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

// Int16 data access
int16_t arrow_get_int16_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return 0;
    }

    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return 0;
    }

    const int16_t* data = (const int16_t*)array->buffers[1];
    return data[index + array->offset];
}

int arrow_is_int16_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

// UInt8 data access
uint8_t arrow_get_uint8_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return 0;
    }

    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return 0;
    }

    const uint8_t* data = (const uint8_t*)array->buffers[1];
    return data[index + array->offset];
}

int arrow_is_uint8_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

// UInt16 data access
uint16_t arrow_get_uint16_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return 0;
    }

    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return 0;
    }

    const uint16_t* data = (const uint16_t*)array->buffers[1];
    return data[index + array->offset];
}

int arrow_is_uint16_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

// UInt32 data access
uint32_t arrow_get_uint32_value(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        return 0;
    }

    if (array->n_buffers <= 1 || array->buffers[1] == NULL) {
        return 0;
    }

    const uint32_t* data = (const uint32_t*)array->buffers[1];
    return data[index + array->offset];
}

int arrow_is_uint32_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}

// Date32 data access (same layout as Int32)
int32_t arrow_get_date32_value(struct ArrowArray* array, size_t index) {
    return arrow_get_int32_value(array, index);
}

int arrow_is_date32_null(struct ArrowArray* array, size_t index) {
    return arrow_is_int32_null(array, index);
}

// Date64 data access (same layout as Int64)
int64_t arrow_get_date64_value(struct ArrowArray* array, size_t index) {
    return arrow_get_int64_value(array, index);
}

int arrow_is_date64_null(struct ArrowArray* array, size_t index) {
    return arrow_is_int64_null(array, index);
}

// Time32 data access (same layout as Int32)
int32_t arrow_get_time32_value(struct ArrowArray* array, size_t index) {
    return arrow_get_int32_value(array, index);
}

int arrow_is_time32_null(struct ArrowArray* array, size_t index) {
    return arrow_is_int32_null(array, index);
}

// Time64 data access (same layout as Int64)
int64_t arrow_get_time64_value(struct ArrowArray* array, size_t index) {
    return arrow_get_int64_value(array, index);
}

int arrow_is_time64_null(struct ArrowArray* array, size_t index) {
    return arrow_is_int64_null(array, index);
}

// Duration data access (same layout as Int64)
int64_t arrow_get_duration_value(struct ArrowArray* array, size_t index) {
    return arrow_get_int64_value(array, index);
}

int arrow_is_duration_null(struct ArrowArray* array, size_t index) {
    return arrow_is_int64_null(array, index);
}

// Binary data access (returns pointer to start and length)
const uint8_t* arrow_get_binary_value(struct ArrowArray* array, size_t index, size_t* out_length) {
    if (!array || index >= (size_t)array->length || is_null(array, index)) {
        if (out_length) *out_length = 0;
        return NULL;
    }

    if (array->n_buffers < 3 || array->buffers[1] == NULL || array->buffers[2] == NULL) {
        if (out_length) *out_length = 0;
        return NULL;
    }

    const int32_t* offsets = (const int32_t*)array->buffers[1];
    const uint8_t* data = (const uint8_t*)array->buffers[2];

    size_t actual_index = index + array->offset;
    int32_t start_offset = offsets[actual_index];
    int32_t end_offset = offsets[actual_index + 1];

    if (out_length) *out_length = (size_t)(end_offset - start_offset);
    return data + start_offset;
}

int arrow_is_binary_null(struct ArrowArray* array, size_t index) {
    if (!array || index >= (size_t)array->length) return -1;
    return is_null(array, index) ? 1 : 0;
}