// Enable POSIX extensions for strdup
#define _POSIX_C_SOURCE 200809L

#include "arrow_nested_builders.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// ============================================================================
// Helper Functions
// ============================================================================

static const char* element_type_to_format(ListElementType type) {
    switch (type) {
        case LIST_ELEM_INT8: return "c";
        case LIST_ELEM_INT16: return "s";
        case LIST_ELEM_INT32: return "i";
        case LIST_ELEM_INT64: return "l";
        case LIST_ELEM_UINT8: return "C";
        case LIST_ELEM_UINT16: return "S";
        case LIST_ELEM_UINT32: return "I";
        case LIST_ELEM_UINT64: return "L";
        case LIST_ELEM_FLOAT32: return "f";
        case LIST_ELEM_FLOAT64: return "g";
        case LIST_ELEM_STRING: return "u";
        case LIST_ELEM_BOOL: return "b";
        case LIST_ELEM_BINARY: return "z";
        default: return "l";
    }
}

static void* create_child_builder(ListElementType type, size_t capacity) {
    switch (type) {
        case LIST_ELEM_INT8: return int8_builder_create(capacity);
        case LIST_ELEM_INT16: return int16_builder_create(capacity);
        case LIST_ELEM_INT32: return int32_builder_create(capacity);
        case LIST_ELEM_INT64: return int64_builder_create(capacity);
        case LIST_ELEM_UINT8: return uint8_builder_create(capacity);
        case LIST_ELEM_UINT16: return uint16_builder_create(capacity);
        case LIST_ELEM_UINT32: return uint32_builder_create(capacity);
        case LIST_ELEM_UINT64: return uint64_builder_create(capacity);
        case LIST_ELEM_FLOAT32: return float32_builder_create(capacity);
        case LIST_ELEM_FLOAT64: return float64_builder_create(capacity);
        case LIST_ELEM_STRING: return string_builder_create(capacity, capacity * 32);
        case LIST_ELEM_BOOL: return bool_builder_create(capacity);
        case LIST_ELEM_BINARY: return binary_builder_create(capacity, capacity * 32);
        default: return NULL;
    }
}

static void free_child_builder(void* builder, ListElementType type) {
    if (!builder) return;
    switch (type) {
        case LIST_ELEM_INT8: int8_builder_free((Int8Builder*)builder); break;
        case LIST_ELEM_INT16: int16_builder_free((Int16Builder*)builder); break;
        case LIST_ELEM_INT32: int32_builder_free((Int32Builder*)builder); break;
        case LIST_ELEM_INT64: int64_builder_free((Int64Builder*)builder); break;
        case LIST_ELEM_UINT8: uint8_builder_free((UInt8Builder*)builder); break;
        case LIST_ELEM_UINT16: uint16_builder_free((UInt16Builder*)builder); break;
        case LIST_ELEM_UINT32: uint32_builder_free((UInt32Builder*)builder); break;
        case LIST_ELEM_UINT64: uint64_builder_free((UInt64Builder*)builder); break;
        case LIST_ELEM_FLOAT32: float32_builder_free((Float32Builder*)builder); break;
        case LIST_ELEM_FLOAT64: float64_builder_free((Float64Builder*)builder); break;
        case LIST_ELEM_STRING: string_builder_free((StringBuilder*)builder); break;
        case LIST_ELEM_BOOL: bool_builder_free((BoolBuilder*)builder); break;
        case LIST_ELEM_BINARY: binary_builder_free((BinaryBuilder*)builder); break;
    }
}

static size_t get_child_builder_length(void* builder, ListElementType type) {
    if (!builder) return 0;
    switch (type) {
        case LIST_ELEM_INT8: return int8_builder_length((Int8Builder*)builder);
        case LIST_ELEM_INT16: return int16_builder_length((Int16Builder*)builder);
        case LIST_ELEM_INT32: return int32_builder_length((Int32Builder*)builder);
        case LIST_ELEM_INT64: return int64_builder_length((Int64Builder*)builder);
        case LIST_ELEM_UINT8: return uint8_builder_length((UInt8Builder*)builder);
        case LIST_ELEM_UINT16: return uint16_builder_length((UInt16Builder*)builder);
        case LIST_ELEM_UINT32: return uint32_builder_length((UInt32Builder*)builder);
        case LIST_ELEM_UINT64: return uint64_builder_length((UInt64Builder*)builder);
        case LIST_ELEM_FLOAT32: return float32_builder_length((Float32Builder*)builder);
        case LIST_ELEM_FLOAT64: return float64_builder_length((Float64Builder*)builder);
        case LIST_ELEM_STRING: return string_builder_length((StringBuilder*)builder);
        case LIST_ELEM_BOOL: return bool_builder_length((BoolBuilder*)builder);
        case LIST_ELEM_BINARY: return binary_builder_length((BinaryBuilder*)builder);
        default: return 0;
    }
}

static struct ArrowArray* finish_child_builder(void* builder, ListElementType type) {
    if (!builder) return NULL;
    switch (type) {
        case LIST_ELEM_INT8: return int8_builder_finish((Int8Builder*)builder);
        case LIST_ELEM_INT16: return int16_builder_finish((Int16Builder*)builder);
        case LIST_ELEM_INT32: return int32_builder_finish((Int32Builder*)builder);
        case LIST_ELEM_INT64: return int64_builder_finish((Int64Builder*)builder);
        case LIST_ELEM_UINT8: return uint8_builder_finish((UInt8Builder*)builder);
        case LIST_ELEM_UINT16: return uint16_builder_finish((UInt16Builder*)builder);
        case LIST_ELEM_UINT32: return uint32_builder_finish((UInt32Builder*)builder);
        case LIST_ELEM_UINT64: return uint64_builder_finish((UInt64Builder*)builder);
        case LIST_ELEM_FLOAT32: return float32_builder_finish((Float32Builder*)builder);
        case LIST_ELEM_FLOAT64: return float64_builder_finish((Float64Builder*)builder);
        case LIST_ELEM_STRING: return string_builder_finish((StringBuilder*)builder);
        case LIST_ELEM_BOOL: return bool_builder_finish((BoolBuilder*)builder);
        case LIST_ELEM_BINARY: return binary_builder_finish((BinaryBuilder*)builder);
        default: return NULL;
    }
}

// ============================================================================
// List Builder Implementation
// ============================================================================

// Private data for list arrays
typedef struct {
    void* validity;
    void* offsets;
    struct ArrowArray* child;
} ListArrayPrivateData;

static void list_array_release(struct ArrowArray* array) {
    if (!array || !array->release) return;

    ListArrayPrivateData* priv = (ListArrayPrivateData*)array->private_data;
    if (priv) {
        free(priv->validity);
        free(priv->offsets);
        if (priv->child && priv->child->release) {
            priv->child->release(priv->child);
        }
        free(priv->child);
        free(priv);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }
    if (array->children) {
        free(array->children);
    }

    array->release = NULL;
}

ListBuilder* list_builder_create(size_t initial_capacity, ListElementType element_type) {
    if (initial_capacity == 0) initial_capacity = 64;

    ListBuilder* builder = calloc(1, sizeof(ListBuilder));
    if (!builder) return NULL;

    builder->offsets = malloc((initial_capacity + 1) * sizeof(int32_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->child_builder = create_child_builder(element_type, initial_capacity * 4);

    if (!builder->offsets || !builder->validity || !builder->child_builder) {
        free(builder->offsets);
        free(builder->validity);
        free_child_builder(builder->child_builder, element_type);
        free(builder);
        return NULL;
    }

    builder->offsets[0] = 0;
    builder->capacity = initial_capacity;
    builder->element_type = element_type;
    builder->in_list = false;

    return builder;
}

static int list_builder_ensure_capacity(ListBuilder* builder) {
    if (builder->length < builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;

    int32_t* new_offsets = realloc(builder->offsets, (new_capacity + 1) * sizeof(int32_t));
    if (!new_offsets) return BUILDER_ERR_ALLOC;
    builder->offsets = new_offsets;

    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int list_builder_start_list(ListBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;
    if (builder->in_list) return BUILDER_ERR_FULL;  // Already in a list

    builder->in_list = true;
    return BUILDER_OK;
}

int list_builder_finish_list(ListBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;
    if (!builder->in_list) return BUILDER_ERR_NULL;

    int result = list_builder_ensure_capacity(builder);
    if (result != BUILDER_OK) return result;

    // Mark as valid
    bitmap_set(builder->validity, builder->length, true);

    // Set offset to current child length
    size_t child_len = get_child_builder_length(builder->child_builder, builder->element_type);
    builder->offsets[builder->length + 1] = (int32_t)child_len;

    builder->length++;
    builder->in_list = false;

    return BUILDER_OK;
}

int list_builder_append_null(ListBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;
    if (builder->in_list) return BUILDER_ERR_FULL;

    int result = list_builder_ensure_capacity(builder);
    if (result != BUILDER_OK) return result;

    // Mark as null
    bitmap_set(builder->validity, builder->length, false);

    // Offset stays the same as previous
    size_t child_len = get_child_builder_length(builder->child_builder, builder->element_type);
    builder->offsets[builder->length + 1] = (int32_t)child_len;

    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

// Type-specific append functions
int list_builder_append_int8(ListBuilder* builder, int8_t value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_INT8) return BUILDER_ERR_NULL;
    return int8_builder_append((Int8Builder*)builder->child_builder, value);
}

int list_builder_append_int16(ListBuilder* builder, int16_t value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_INT16) return BUILDER_ERR_NULL;
    return int16_builder_append((Int16Builder*)builder->child_builder, value);
}

int list_builder_append_int32(ListBuilder* builder, int32_t value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_INT32) return BUILDER_ERR_NULL;
    return int32_builder_append((Int32Builder*)builder->child_builder, value);
}

int list_builder_append_int64(ListBuilder* builder, int64_t value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_INT64) return BUILDER_ERR_NULL;
    return int64_builder_append((Int64Builder*)builder->child_builder, value);
}

int list_builder_append_uint8(ListBuilder* builder, uint8_t value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_UINT8) return BUILDER_ERR_NULL;
    return uint8_builder_append((UInt8Builder*)builder->child_builder, value);
}

int list_builder_append_uint16(ListBuilder* builder, uint16_t value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_UINT16) return BUILDER_ERR_NULL;
    return uint16_builder_append((UInt16Builder*)builder->child_builder, value);
}

int list_builder_append_uint32(ListBuilder* builder, uint32_t value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_UINT32) return BUILDER_ERR_NULL;
    return uint32_builder_append((UInt32Builder*)builder->child_builder, value);
}

int list_builder_append_uint64(ListBuilder* builder, uint64_t value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_UINT64) return BUILDER_ERR_NULL;
    return uint64_builder_append((UInt64Builder*)builder->child_builder, value);
}

int list_builder_append_float32(ListBuilder* builder, float value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_FLOAT32) return BUILDER_ERR_NULL;
    return float32_builder_append((Float32Builder*)builder->child_builder, value);
}

int list_builder_append_float64(ListBuilder* builder, double value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_FLOAT64) return BUILDER_ERR_NULL;
    return float64_builder_append((Float64Builder*)builder->child_builder, value);
}

int list_builder_append_string(ListBuilder* builder, const char* value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_STRING) return BUILDER_ERR_NULL;
    return string_builder_append_cstr((StringBuilder*)builder->child_builder, value);
}

int list_builder_append_bool(ListBuilder* builder, bool value) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_BOOL) return BUILDER_ERR_NULL;
    return bool_builder_append((BoolBuilder*)builder->child_builder, value);
}

int list_builder_append_binary(ListBuilder* builder, const uint8_t* data, size_t length) {
    if (!builder || !builder->in_list) return BUILDER_ERR_NULL;
    if (builder->element_type != LIST_ELEM_BINARY) return BUILDER_ERR_NULL;
    return binary_builder_append((BinaryBuilder*)builder->child_builder, data, length);
}

const char* list_builder_child_format(ListBuilder* builder) {
    if (!builder) return "l";
    return element_type_to_format(builder->element_type);
}

struct ArrowArray* list_builder_finish(ListBuilder* builder) {
    if (!builder) return NULL;
    if (builder->in_list) return NULL;  // Can't finish while building a list

    // Finish the child builder first
    struct ArrowArray* child = finish_child_builder(builder->child_builder, builder->element_type);
    if (!child) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) {
        if (child->release) child->release(child);
        free(child);
        return NULL;
    }

    ListArrayPrivateData* priv = calloc(1, sizeof(ListArrayPrivateData));
    if (!priv) {
        if (child->release) child->release(child);
        free(child);
        free(array);
        return NULL;
    }

    // Allocate buffers array (validity + offsets)
    const void** buffers = malloc(2 * sizeof(void*));
    if (!buffers) {
        if (child->release) child->release(child);
        free(child);
        free(priv);
        free(array);
        return NULL;
    }

    // Allocate children array
    struct ArrowArray** children = malloc(sizeof(struct ArrowArray*));
    if (!children) {
        if (child->release) child->release(child);
        free(child);
        free(priv);
        free(buffers);
        free(array);
        return NULL;
    }

    priv->validity = builder->validity;
    priv->offsets = builder->offsets;
    priv->child = child;

    buffers[0] = builder->null_count > 0 ? builder->validity : NULL;
    buffers[1] = builder->offsets;

    children[0] = child;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 1;
    array->buffers = buffers;
    array->children = children;
    array->dictionary = NULL;
    array->private_data = priv;
    array->release = list_array_release;

    // Clear builder ownership
    builder->validity = NULL;
    builder->offsets = NULL;
    builder->child_builder = NULL;

    return array;
}

void list_builder_free(ListBuilder* builder) {
    if (!builder) return;
    free(builder->validity);
    free(builder->offsets);
    free_child_builder(builder->child_builder, builder->element_type);
    free(builder);
}

size_t list_builder_length(const ListBuilder* builder) {
    return builder ? builder->length : 0;
}

// ============================================================================
// Struct Builder Implementation
// ============================================================================

typedef struct {
    void* validity;
    struct ArrowArray** children;
    size_t num_children;
} StructArrayPrivateData;

static void struct_array_release(struct ArrowArray* array) {
    if (!array || !array->release) return;

    StructArrayPrivateData* priv = (StructArrayPrivateData*)array->private_data;
    if (priv) {
        free(priv->validity);
        if (priv->children) {
            for (size_t i = 0; i < priv->num_children; i++) {
                if (priv->children[i] && priv->children[i]->release) {
                    priv->children[i]->release(priv->children[i]);
                }
                free(priv->children[i]);
            }
            free(priv->children);
        }
        free(priv);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }
    if (array->children) {
        free(array->children);
    }

    array->release = NULL;
}

StructBuilder* struct_builder_create(size_t initial_capacity) {
    if (initial_capacity == 0) initial_capacity = 64;

    StructBuilder* builder = calloc(1, sizeof(StructBuilder));
    if (!builder) return NULL;

    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    if (!builder->validity) {
        free(builder);
        return NULL;
    }

    builder->capacity = initial_capacity;
    return builder;
}

int struct_builder_add_field(StructBuilder* builder, const char* name, ListElementType type) {
    if (!builder || !name) return BUILDER_ERR_NULL;

    size_t new_count = builder->num_fields + 1;

    void** new_builders = realloc(builder->field_builders, new_count * sizeof(void*));
    char** new_names = realloc(builder->field_names, new_count * sizeof(char*));
    char** new_formats = realloc(builder->field_formats, new_count * sizeof(char*));
    ListElementType* new_types = realloc(builder->field_types, new_count * sizeof(ListElementType));

    if (!new_builders || !new_names || !new_formats || !new_types) {
        return BUILDER_ERR_ALLOC;
    }

    builder->field_builders = new_builders;
    builder->field_names = new_names;
    builder->field_formats = new_formats;
    builder->field_types = new_types;

    // Create child builder
    void* child = create_child_builder(type, builder->capacity);
    if (!child) return BUILDER_ERR_ALLOC;

    builder->field_builders[builder->num_fields] = child;
    builder->field_names[builder->num_fields] = strdup(name);
    builder->field_formats[builder->num_fields] = strdup(element_type_to_format(type));
    builder->field_types[builder->num_fields] = type;

    if (!builder->field_names[builder->num_fields] || !builder->field_formats[builder->num_fields]) {
        free_child_builder(child, type);
        return BUILDER_ERR_ALLOC;
    }

    builder->num_fields = new_count;
    return BUILDER_OK;
}

static int struct_builder_ensure_capacity(StructBuilder* builder) {
    if (builder->length < builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;

    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

// Type-specific struct append functions
int struct_builder_append_int8(StructBuilder* builder, size_t field_idx, int8_t value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_INT8) return BUILDER_ERR_NULL;
    return int8_builder_append((Int8Builder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_int16(StructBuilder* builder, size_t field_idx, int16_t value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_INT16) return BUILDER_ERR_NULL;
    return int16_builder_append((Int16Builder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_int32(StructBuilder* builder, size_t field_idx, int32_t value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_INT32) return BUILDER_ERR_NULL;
    return int32_builder_append((Int32Builder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_int64(StructBuilder* builder, size_t field_idx, int64_t value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_INT64) return BUILDER_ERR_NULL;
    return int64_builder_append((Int64Builder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_uint8(StructBuilder* builder, size_t field_idx, uint8_t value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_UINT8) return BUILDER_ERR_NULL;
    return uint8_builder_append((UInt8Builder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_uint16(StructBuilder* builder, size_t field_idx, uint16_t value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_UINT16) return BUILDER_ERR_NULL;
    return uint16_builder_append((UInt16Builder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_uint32(StructBuilder* builder, size_t field_idx, uint32_t value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_UINT32) return BUILDER_ERR_NULL;
    return uint32_builder_append((UInt32Builder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_uint64(StructBuilder* builder, size_t field_idx, uint64_t value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_UINT64) return BUILDER_ERR_NULL;
    return uint64_builder_append((UInt64Builder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_float32(StructBuilder* builder, size_t field_idx, float value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_FLOAT32) return BUILDER_ERR_NULL;
    return float32_builder_append((Float32Builder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_float64(StructBuilder* builder, size_t field_idx, double value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_FLOAT64) return BUILDER_ERR_NULL;
    return float64_builder_append((Float64Builder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_string(StructBuilder* builder, size_t field_idx, const char* value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_STRING) return BUILDER_ERR_NULL;
    return string_builder_append_cstr((StringBuilder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_bool(StructBuilder* builder, size_t field_idx, bool value) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_BOOL) return BUILDER_ERR_NULL;
    return bool_builder_append((BoolBuilder*)builder->field_builders[field_idx], value);
}

int struct_builder_append_binary(StructBuilder* builder, size_t field_idx, const uint8_t* data, size_t length) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;
    if (builder->field_types[field_idx] != LIST_ELEM_BINARY) return BUILDER_ERR_NULL;
    return binary_builder_append((BinaryBuilder*)builder->field_builders[field_idx], data, length);
}

int struct_builder_append_field_null(StructBuilder* builder, size_t field_idx) {
    if (!builder || field_idx >= builder->num_fields) return BUILDER_ERR_NULL;

    ListElementType type = builder->field_types[field_idx];
    void* child = builder->field_builders[field_idx];

    switch (type) {
        case LIST_ELEM_INT8: return int8_builder_append_null((Int8Builder*)child);
        case LIST_ELEM_INT16: return int16_builder_append_null((Int16Builder*)child);
        case LIST_ELEM_INT32: return int32_builder_append_null((Int32Builder*)child);
        case LIST_ELEM_INT64: return int64_builder_append_null((Int64Builder*)child);
        case LIST_ELEM_UINT8: return uint8_builder_append_null((UInt8Builder*)child);
        case LIST_ELEM_UINT16: return uint16_builder_append_null((UInt16Builder*)child);
        case LIST_ELEM_UINT32: return uint32_builder_append_null((UInt32Builder*)child);
        case LIST_ELEM_UINT64: return uint64_builder_append_null((UInt64Builder*)child);
        case LIST_ELEM_FLOAT32: return float32_builder_append_null((Float32Builder*)child);
        case LIST_ELEM_FLOAT64: return float64_builder_append_null((Float64Builder*)child);
        case LIST_ELEM_STRING: return string_builder_append_null((StringBuilder*)child);
        case LIST_ELEM_BOOL: return bool_builder_append_null((BoolBuilder*)child);
        case LIST_ELEM_BINARY: return binary_builder_append_null((BinaryBuilder*)child);
        default: return BUILDER_ERR_NULL;
    }
}

int struct_builder_append_null(StructBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int result = struct_builder_ensure_capacity(builder);
    if (result != BUILDER_OK) return result;

    // Append null to all fields
    for (size_t i = 0; i < builder->num_fields; i++) {
        result = struct_builder_append_field_null(builder, i);
        if (result != BUILDER_OK) return result;
    }

    // Mark struct as null
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

int struct_builder_finish_row(StructBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int result = struct_builder_ensure_capacity(builder);
    if (result != BUILDER_OK) return result;

    // Mark as valid
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

struct ArrowArray* struct_builder_finish(StructBuilder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    StructArrayPrivateData* priv = calloc(1, sizeof(StructArrayPrivateData));
    if (!priv) {
        free(array);
        return NULL;
    }

    // Finish all child builders
    struct ArrowArray** children = calloc(builder->num_fields, sizeof(struct ArrowArray*));
    if (!children) {
        free(priv);
        free(array);
        return NULL;
    }

    for (size_t i = 0; i < builder->num_fields; i++) {
        children[i] = finish_child_builder(builder->field_builders[i], builder->field_types[i]);
        if (!children[i]) {
            // Cleanup
            for (size_t j = 0; j < i; j++) {
                if (children[j] && children[j]->release) {
                    children[j]->release(children[j]);
                }
                free(children[j]);
            }
            free(children);
            free(priv);
            free(array);
            return NULL;
        }
    }

    // Allocate buffers array (just validity)
    const void** buffers = malloc(sizeof(void*));
    if (!buffers) {
        for (size_t i = 0; i < builder->num_fields; i++) {
            if (children[i] && children[i]->release) {
                children[i]->release(children[i]);
            }
            free(children[i]);
        }
        free(children);
        free(priv);
        free(array);
        return NULL;
    }

    priv->validity = builder->validity;
    priv->children = children;
    priv->num_children = builder->num_fields;

    buffers[0] = builder->null_count > 0 ? builder->validity : NULL;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 1;
    array->n_children = (int64_t)builder->num_fields;
    array->buffers = buffers;
    array->children = children;
    array->dictionary = NULL;
    array->private_data = priv;
    array->release = struct_array_release;

    // Clear builder ownership
    builder->validity = NULL;
    for (size_t i = 0; i < builder->num_fields; i++) {
        builder->field_builders[i] = NULL;
    }

    return array;
}

static void struct_schema_release_fn(struct ArrowSchema* schema) {
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

struct ArrowSchema* struct_builder_get_schema(StructBuilder* builder) {
    if (!builder) return NULL;

    struct ArrowSchema* schema = calloc(1, sizeof(struct ArrowSchema));
    if (!schema) return NULL;

    schema->format = strdup("+s");
    schema->name = NULL;
    schema->metadata = NULL;
    schema->flags = 0;
    schema->n_children = (int64_t)builder->num_fields;
    schema->dictionary = NULL;

    if (builder->num_fields > 0) {
        schema->children = calloc(builder->num_fields, sizeof(struct ArrowSchema*));
        if (!schema->children) {
            free((void*)schema->format);
            free(schema);
            return NULL;
        }

        for (size_t i = 0; i < builder->num_fields; i++) {
            struct ArrowSchema* child = calloc(1, sizeof(struct ArrowSchema));
            if (!child) {
                // Cleanup
                for (size_t j = 0; j < i; j++) {
                    if (schema->children[j] && schema->children[j]->release) {
                        schema->children[j]->release(schema->children[j]);
                    }
                    free(schema->children[j]);
                }
                free(schema->children);
                free((void*)schema->format);
                free(schema);
                return NULL;
            }

            child->format = strdup(builder->field_formats[i]);
            child->name = strdup(builder->field_names[i]);
            child->metadata = NULL;
            child->flags = ARROW_FLAG_NULLABLE;
            child->n_children = 0;
            child->children = NULL;
            child->dictionary = NULL;
            child->release = struct_schema_release_fn;

            schema->children[i] = child;
        }
    }

    schema->release = struct_schema_release_fn;
    return schema;
}

void struct_builder_free(StructBuilder* builder) {
    if (!builder) return;

    free(builder->validity);

    for (size_t i = 0; i < builder->num_fields; i++) {
        free_child_builder(builder->field_builders[i], builder->field_types[i]);
        free(builder->field_names[i]);
        free(builder->field_formats[i]);
    }

    free(builder->field_builders);
    free(builder->field_names);
    free(builder->field_formats);
    free(builder->field_types);
    free(builder);
}

size_t struct_builder_length(const StructBuilder* builder) {
    return builder ? builder->length : 0;
}

size_t struct_builder_field_count(const StructBuilder* builder) {
    return builder ? builder->num_fields : 0;
}

// ============================================================================
// Decimal128 Builder Implementation
// ============================================================================

typedef struct {
    void* validity;
    void* values;
} Decimal128ArrayPrivateData;

static void decimal128_array_release(struct ArrowArray* array) {
    if (!array || !array->release) return;

    Decimal128ArrayPrivateData* priv = (Decimal128ArrayPrivateData*)array->private_data;
    if (priv) {
        free(priv->validity);
        free(priv->values);
        free(priv);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }

    array->release = NULL;
}

Decimal128Builder* decimal128_builder_create(size_t initial_capacity, int32_t precision, int32_t scale) {
    if (initial_capacity == 0) initial_capacity = 64;
    if (precision < 1 || precision > 38) return NULL;
    if (scale < 0 || scale > precision) return NULL;

    Decimal128Builder* builder = calloc(1, sizeof(Decimal128Builder));
    if (!builder) return NULL;

    builder->values = calloc(initial_capacity, 16);  // 16 bytes per decimal128
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);

    if (!builder->values || !builder->validity) {
        free(builder->values);
        free(builder->validity);
        free(builder);
        return NULL;
    }

    builder->capacity = initial_capacity;
    builder->precision = precision;
    builder->scale = scale;

    return builder;
}

static int decimal128_builder_ensure_capacity(Decimal128Builder* builder) {
    if (builder->length < builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;

    uint8_t* new_values = realloc(builder->values, new_capacity * 16);
    if (!new_values) return BUILDER_ERR_ALLOC;
    memset(new_values + builder->capacity * 16, 0, builder->capacity * 16);
    builder->values = new_values;

    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

int decimal128_builder_append(Decimal128Builder* builder, int64_t high, uint64_t low) {
    if (!builder) return BUILDER_ERR_NULL;

    int result = decimal128_builder_ensure_capacity(builder);
    if (result != BUILDER_OK) return result;

    // Store as little-endian 128-bit value
    uint8_t* dest = builder->values + builder->length * 16;
    memcpy(dest, &low, 8);
    memcpy(dest + 8, &high, 8);

    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int decimal128_builder_append_string(Decimal128Builder* builder, const char* value) {
    if (!builder || !value) return BUILDER_ERR_NULL;

    // Parse string to decimal128
    // This is a simplified implementation - a full one would handle all edge cases
    int64_t high = 0;
    uint64_t low = 0;

    bool negative = false;
    const char* p = value;

    if (*p == '-') {
        negative = true;
        p++;
    } else if (*p == '+') {
        p++;
    }

    // Parse integer part
    __int128 result = 0;
    int decimal_places = 0;
    bool seen_decimal = false;

    while (*p) {
        if (*p == '.') {
            if (seen_decimal) return BUILDER_ERR_NULL;  // Multiple decimal points
            seen_decimal = true;
        } else if (*p >= '0' && *p <= '9') {
            result = result * 10 + (*p - '0');
            if (seen_decimal) decimal_places++;
        } else {
            return BUILDER_ERR_NULL;  // Invalid character
        }
        p++;
    }

    // Adjust for scale
    while (decimal_places < builder->scale) {
        result *= 10;
        decimal_places++;
    }
    while (decimal_places > builder->scale) {
        result /= 10;
        decimal_places--;
    }

    if (negative) result = -result;

    low = (uint64_t)result;
    high = (int64_t)(result >> 64);

    return decimal128_builder_append(builder, high, low);
}

int decimal128_builder_append_null(Decimal128Builder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int result = decimal128_builder_ensure_capacity(builder);
    if (result != BUILDER_OK) return result;

    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* decimal128_builder_finish(Decimal128Builder* builder) {
    if (!builder) return NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;

    Decimal128ArrayPrivateData* priv = calloc(1, sizeof(Decimal128ArrayPrivateData));
    if (!priv) {
        free(array);
        return NULL;
    }

    const void** buffers = malloc(2 * sizeof(void*));
    if (!buffers) {
        free(priv);
        free(array);
        return NULL;
    }

    priv->validity = builder->validity;
    priv->values = builder->values;

    buffers[0] = builder->null_count > 0 ? builder->validity : NULL;
    buffers[1] = builder->values;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->buffers = buffers;
    array->children = NULL;
    array->dictionary = NULL;
    array->private_data = priv;
    array->release = decimal128_array_release;

    builder->validity = NULL;
    builder->values = NULL;

    return array;
}

void decimal128_builder_free(Decimal128Builder* builder) {
    if (!builder) return;
    free(builder->validity);
    free(builder->values);
    free(builder);
}

size_t decimal128_builder_length(const Decimal128Builder* builder) {
    return builder ? builder->length : 0;
}

int32_t decimal128_builder_precision(const Decimal128Builder* builder) {
    return builder ? builder->precision : 0;
}

int32_t decimal128_builder_scale(const Decimal128Builder* builder) {
    return builder ? builder->scale : 0;
}

// ============================================================================
// Dictionary Builder Implementation
// ============================================================================

// Simple hash function for strings
static uint32_t dict_hash(const char* str, size_t capacity) {
    uint32_t hash = 5381;
    while (*str) {
        hash = ((hash << 5) + hash) + (uint8_t)*str;
        str++;
    }
    return hash % capacity;
}

typedef struct {
    void* validity;
    void* indices;
    struct ArrowArray* dictionary;
} DictArrayPrivateData;

static void dict_array_release(struct ArrowArray* array) {
    if (!array || !array->release) return;

    DictArrayPrivateData* priv = (DictArrayPrivateData*)array->private_data;
    if (priv) {
        free(priv->validity);
        free(priv->indices);
        if (priv->dictionary && priv->dictionary->release) {
            priv->dictionary->release(priv->dictionary);
        }
        free(priv->dictionary);
        free(priv);
    }

    if (array->buffers) {
        free((void*)array->buffers);
    }

    array->release = NULL;
}

DictionaryBuilder* dictionary_builder_create(size_t initial_capacity, size_t dict_capacity) {
    if (initial_capacity == 0) initial_capacity = 64;
    if (dict_capacity == 0) dict_capacity = 1024;

    DictionaryBuilder* builder = calloc(1, sizeof(DictionaryBuilder));
    if (!builder) return NULL;

    builder->indices = malloc(initial_capacity * sizeof(int32_t));
    builder->validity = calloc(bitmap_byte_count(initial_capacity), 1);
    builder->dictionary = string_builder_create(dict_capacity, dict_capacity * 32);

    // Hash table for deduplication
    builder->hash_capacity = dict_capacity * 2;
    builder->hash_keys = calloc(builder->hash_capacity, sizeof(char*));
    builder->hash_values = calloc(builder->hash_capacity, sizeof(int32_t));

    if (!builder->indices || !builder->validity || !builder->dictionary ||
        !builder->hash_keys || !builder->hash_values) {
        free(builder->indices);
        free(builder->validity);
        if (builder->dictionary) string_builder_free(builder->dictionary);
        free(builder->hash_keys);
        free(builder->hash_values);
        free(builder);
        return NULL;
    }

    // Initialize hash values to -1 (no entry)
    for (size_t i = 0; i < builder->hash_capacity; i++) {
        builder->hash_values[i] = -1;
    }

    builder->capacity = initial_capacity;
    return builder;
}

static int dictionary_builder_ensure_capacity(DictionaryBuilder* builder) {
    if (builder->length < builder->capacity) return BUILDER_OK;

    size_t new_capacity = builder->capacity * 2;

    int32_t* new_indices = realloc(builder->indices, new_capacity * sizeof(int32_t));
    if (!new_indices) return BUILDER_ERR_ALLOC;
    builder->indices = new_indices;

    size_t old_bitmap_size = bitmap_byte_count(builder->capacity);
    size_t new_bitmap_size = bitmap_byte_count(new_capacity);
    uint8_t* new_validity = realloc(builder->validity, new_bitmap_size);
    if (!new_validity) return BUILDER_ERR_ALLOC;
    memset(new_validity + old_bitmap_size, 0, new_bitmap_size - old_bitmap_size);
    builder->validity = new_validity;

    builder->capacity = new_capacity;
    return BUILDER_OK;
}

static int32_t dictionary_builder_get_or_insert(DictionaryBuilder* builder, const char* value) {
    // Look up in hash table
    uint32_t hash = dict_hash(value, builder->hash_capacity);
    uint32_t original_hash = hash;

    while (builder->hash_keys[hash] != NULL) {
        if (strcmp(builder->hash_keys[hash], value) == 0) {
            return builder->hash_values[hash];
        }
        hash = (hash + 1) % builder->hash_capacity;
        if (hash == original_hash) {
            // Table is full - should not happen with proper sizing
            return -1;
        }
    }

    // Not found - insert into dictionary
    int32_t index = (int32_t)string_builder_length(builder->dictionary);
    if (string_builder_append_cstr(builder->dictionary, value) != BUILDER_OK) {
        return -1;
    }

    // Insert into hash table
    builder->hash_keys[hash] = strdup(value);
    builder->hash_values[hash] = index;

    return index;
}

int dictionary_builder_append(DictionaryBuilder* builder, const char* value) {
    if (!builder || !value) return BUILDER_ERR_NULL;

    int result = dictionary_builder_ensure_capacity(builder);
    if (result != BUILDER_OK) return result;

    int32_t index = dictionary_builder_get_or_insert(builder, value);
    if (index < 0) return BUILDER_ERR_ALLOC;

    builder->indices[builder->length] = index;
    bitmap_set(builder->validity, builder->length, true);
    builder->length++;

    return BUILDER_OK;
}

int dictionary_builder_append_null(DictionaryBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;

    int result = dictionary_builder_ensure_capacity(builder);
    if (result != BUILDER_OK) return result;

    builder->indices[builder->length] = 0;  // Index doesn't matter for null
    bitmap_set(builder->validity, builder->length, false);
    builder->length++;
    builder->null_count++;

    return BUILDER_OK;
}

struct ArrowArray* dictionary_builder_finish(DictionaryBuilder* builder, struct ArrowArray** out_dictionary) {
    if (!builder) return NULL;

    // Finish the dictionary string builder
    struct ArrowArray* dict = string_builder_finish(builder->dictionary);
    if (!dict) return NULL;
    builder->dictionary = NULL;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) {
        if (dict->release) dict->release(dict);
        free(dict);
        return NULL;
    }

    DictArrayPrivateData* priv = calloc(1, sizeof(DictArrayPrivateData));
    if (!priv) {
        if (dict->release) dict->release(dict);
        free(dict);
        free(array);
        return NULL;
    }

    const void** buffers = malloc(2 * sizeof(void*));
    if (!buffers) {
        if (dict->release) dict->release(dict);
        free(dict);
        free(priv);
        free(array);
        return NULL;
    }

    priv->validity = builder->validity;
    priv->indices = builder->indices;
    priv->dictionary = dict;

    buffers[0] = builder->null_count > 0 ? builder->validity : NULL;
    buffers[1] = builder->indices;

    array->length = (int64_t)builder->length;
    array->null_count = (int64_t)builder->null_count;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->buffers = buffers;
    array->children = NULL;
    array->dictionary = dict;  // Set dictionary pointer
    array->private_data = priv;
    array->release = dict_array_release;

    if (out_dictionary) {
        *out_dictionary = dict;
    }

    builder->validity = NULL;
    builder->indices = NULL;

    return array;
}

void dictionary_builder_free(DictionaryBuilder* builder) {
    if (!builder) return;

    free(builder->validity);
    free(builder->indices);
    if (builder->dictionary) string_builder_free(builder->dictionary);

    // Free hash table keys
    for (size_t i = 0; i < builder->hash_capacity; i++) {
        free(builder->hash_keys[i]);
    }
    free(builder->hash_keys);
    free(builder->hash_values);

    free(builder);
}

size_t dictionary_builder_length(const DictionaryBuilder* builder) {
    return builder ? builder->length : 0;
}

size_t dictionary_builder_dict_size(const DictionaryBuilder* builder) {
    return builder && builder->dictionary ? string_builder_length(builder->dictionary) : 0;
}

// ============================================================================
// Map Builder Implementation (simplified)
// ============================================================================

// Map is implemented as List<Struct<key, value>>
// For simplicity, we'll implement just string->string and string->int64 maps

MapBuilder* map_builder_create(size_t initial_capacity, ListElementType key_type, ListElementType value_type) {
    // For now, we only support string keys
    if (key_type != LIST_ELEM_STRING && key_type != LIST_ELEM_INT64) {
        return NULL;
    }

    MapBuilder* builder = calloc(1, sizeof(MapBuilder));
    if (!builder) return NULL;

    // Create struct builder for key-value pairs
    builder->entry_builder = struct_builder_create(initial_capacity * 4);
    if (!builder->entry_builder) {
        free(builder);
        return NULL;
    }

    // Add key and value fields
    if (struct_builder_add_field(builder->entry_builder, "key", key_type) != BUILDER_OK ||
        struct_builder_add_field(builder->entry_builder, "value", value_type) != BUILDER_OK) {
        struct_builder_free(builder->entry_builder);
        free(builder);
        return NULL;
    }

    builder->key_type = key_type;
    builder->value_type = value_type;

    // List builder will be created on first use
    // For now, just track offsets manually
    builder->list_builder = NULL;

    return builder;
}

int map_builder_start_map(MapBuilder* builder) {
    // For maps, we don't need to do anything special - entries are added directly
    return BUILDER_OK;
}

int map_builder_finish_map(MapBuilder* builder) {
    // Mark end of map - for now, this is a no-op since we track entries differently
    return BUILDER_OK;
}

int map_builder_append_null(MapBuilder* builder) {
    if (!builder) return BUILDER_ERR_NULL;
    return struct_builder_append_null(builder->entry_builder);
}

int map_builder_append_string_int64(MapBuilder* builder, const char* key, int64_t value) {
    if (!builder) return BUILDER_ERR_NULL;
    if (builder->key_type != LIST_ELEM_STRING || builder->value_type != LIST_ELEM_INT64) {
        return BUILDER_ERR_NULL;
    }

    int result = struct_builder_append_string(builder->entry_builder, 0, key);
    if (result != BUILDER_OK) return result;

    result = struct_builder_append_int64(builder->entry_builder, 1, value);
    if (result != BUILDER_OK) return result;

    return struct_builder_finish_row(builder->entry_builder);
}

int map_builder_append_string_string(MapBuilder* builder, const char* key, const char* value) {
    if (!builder) return BUILDER_ERR_NULL;
    if (builder->key_type != LIST_ELEM_STRING || builder->value_type != LIST_ELEM_STRING) {
        return BUILDER_ERR_NULL;
    }

    int result = struct_builder_append_string(builder->entry_builder, 0, key);
    if (result != BUILDER_OK) return result;

    result = struct_builder_append_string(builder->entry_builder, 1, value);
    if (result != BUILDER_OK) return result;

    return struct_builder_finish_row(builder->entry_builder);
}

int map_builder_append_string_float64(MapBuilder* builder, const char* key, double value) {
    if (!builder) return BUILDER_ERR_NULL;
    if (builder->key_type != LIST_ELEM_STRING || builder->value_type != LIST_ELEM_FLOAT64) {
        return BUILDER_ERR_NULL;
    }

    int result = struct_builder_append_string(builder->entry_builder, 0, key);
    if (result != BUILDER_OK) return result;

    result = struct_builder_append_float64(builder->entry_builder, 1, value);
    if (result != BUILDER_OK) return result;

    return struct_builder_finish_row(builder->entry_builder);
}

int map_builder_append_int64_int64(MapBuilder* builder, int64_t key, int64_t value) {
    if (!builder) return BUILDER_ERR_NULL;
    if (builder->key_type != LIST_ELEM_INT64 || builder->value_type != LIST_ELEM_INT64) {
        return BUILDER_ERR_NULL;
    }

    int result = struct_builder_append_int64(builder->entry_builder, 0, key);
    if (result != BUILDER_OK) return result;

    result = struct_builder_append_int64(builder->entry_builder, 1, value);
    if (result != BUILDER_OK) return result;

    return struct_builder_finish_row(builder->entry_builder);
}

int map_builder_append_int64_string(MapBuilder* builder, int64_t key, const char* value) {
    if (!builder) return BUILDER_ERR_NULL;
    if (builder->key_type != LIST_ELEM_INT64 || builder->value_type != LIST_ELEM_STRING) {
        return BUILDER_ERR_NULL;
    }

    int result = struct_builder_append_int64(builder->entry_builder, 0, key);
    if (result != BUILDER_OK) return result;

    result = struct_builder_append_string(builder->entry_builder, 1, value);
    if (result != BUILDER_OK) return result;

    return struct_builder_finish_row(builder->entry_builder);
}

struct ArrowArray* map_builder_finish(MapBuilder* builder) {
    if (!builder) return NULL;

    // Return the underlying struct array
    // In a full implementation, this would be wrapped in a List array
    return struct_builder_finish(builder->entry_builder);
}

void map_builder_free(MapBuilder* builder) {
    if (!builder) return;

    if (builder->entry_builder) struct_builder_free(builder->entry_builder);
    if (builder->list_builder) list_builder_free(builder->list_builder);
    free(builder);
}

size_t map_builder_length(const MapBuilder* builder) {
    return builder && builder->entry_builder ? struct_builder_length(builder->entry_builder) : 0;
}
