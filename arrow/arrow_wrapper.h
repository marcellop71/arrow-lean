#ifndef ARROW_WRAPPER_H
#define ARROW_WRAPPER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "arrow_c_abi.h"

#ifdef __cplusplus
extern "C" {
#endif

// Schema operations
struct ArrowSchema* arrow_schema_init(const char* format);
int arrow_schema_add_child(struct ArrowSchema* schema, struct ArrowSchema* child);
void arrow_schema_release(struct ArrowSchema* schema);

// Array operations
struct ArrowArray* arrow_array_init(int64_t length);
int arrow_array_set_buffer(struct ArrowArray* array, size_t index, uint8_t* buffer);
uint8_t* arrow_array_get_buffer(struct ArrowArray* array, size_t index);
void arrow_array_release(struct ArrowArray* array);

// Stream operations
struct ArrowArrayStream* arrow_stream_init(void);
struct ArrowSchema* arrow_stream_get_schema(struct ArrowArrayStream* stream);
struct ArrowArray* arrow_stream_get_next(struct ArrowArrayStream* stream);

// Data access functions
bool arrow_get_bool_value(struct ArrowArray* array, size_t index);
int64_t arrow_get_int64_value(struct ArrowArray* array, size_t index);
int32_t arrow_get_int32_value(struct ArrowArray* array, size_t index);
uint64_t arrow_get_uint64_value(struct ArrowArray* array, size_t index);
double arrow_get_float64_value(struct ArrowArray* array, size_t index);
float arrow_get_float32_value(struct ArrowArray* array, size_t index);
const char* arrow_get_string_value(struct ArrowArray* array, size_t index);

// Null checking functions
int arrow_is_bool_null(struct ArrowArray* array, size_t index);
int arrow_is_int64_null(struct ArrowArray* array, size_t index);
int arrow_is_int32_null(struct ArrowArray* array, size_t index);
int arrow_is_uint64_null(struct ArrowArray* array, size_t index);
int arrow_is_float64_null(struct ArrowArray* array, size_t index);
int arrow_is_float32_null(struct ArrowArray* array, size_t index);
int arrow_is_string_null(struct ArrowArray* array, size_t index);

// Buffer management
struct ArrowBuffer* arrow_allocate_buffer(size_t size);
int arrow_buffer_resize(struct ArrowBuffer* buffer, size_t new_size);
void arrow_buffer_free(struct ArrowBuffer* buffer);
int arrow_buffer_write(struct ArrowBuffer* buffer, size_t offset, const void* data, size_t data_size);
int arrow_buffer_read(struct ArrowBuffer* buffer, size_t offset, void* data, size_t data_size);
size_t arrow_buffer_get_size(struct ArrowBuffer* buffer);
size_t arrow_buffer_get_capacity(struct ArrowBuffer* buffer);
uint8_t* arrow_buffer_get_data(struct ArrowBuffer* buffer);

#ifdef __cplusplus
}
#endif

#endif // ARROW_WRAPPER_H