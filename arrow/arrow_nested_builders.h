#ifndef ARROW_NESTED_BUILDERS_H
#define ARROW_NESTED_BUILDERS_H

#include "arrow_c_abi.h"
#include "arrow_builders.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// List Builder
// ============================================================================

// Element type enum for list builder
typedef enum {
    LIST_ELEM_INT8,
    LIST_ELEM_INT16,
    LIST_ELEM_INT32,
    LIST_ELEM_INT64,
    LIST_ELEM_UINT8,
    LIST_ELEM_UINT16,
    LIST_ELEM_UINT32,
    LIST_ELEM_UINT64,
    LIST_ELEM_FLOAT32,
    LIST_ELEM_FLOAT64,
    LIST_ELEM_STRING,
    LIST_ELEM_BOOL,
    LIST_ELEM_BINARY
} ListElementType;

typedef struct {
    int32_t* offsets;           // Offset buffer (length + 1 elements)
    void* child_builder;        // Typed child builder
    uint8_t* validity;          // Null bitmap for lists
    size_t length;              // Number of lists
    size_t capacity;            // Capacity for lists (offsets)
    size_t null_count;
    ListElementType element_type;
    bool in_list;               // Currently building a list?
} ListBuilder;

// Create a list builder for a specific element type
ListBuilder* list_builder_create(size_t initial_capacity, ListElementType element_type);

// Start building a new list element
int list_builder_start_list(ListBuilder* builder);

// Finish the current list element
int list_builder_finish_list(ListBuilder* builder);

// Append a null list
int list_builder_append_null(ListBuilder* builder);

// Append values to current list (type-specific)
int list_builder_append_int8(ListBuilder* builder, int8_t value);
int list_builder_append_int16(ListBuilder* builder, int16_t value);
int list_builder_append_int32(ListBuilder* builder, int32_t value);
int list_builder_append_int64(ListBuilder* builder, int64_t value);
int list_builder_append_uint8(ListBuilder* builder, uint8_t value);
int list_builder_append_uint16(ListBuilder* builder, uint16_t value);
int list_builder_append_uint32(ListBuilder* builder, uint32_t value);
int list_builder_append_uint64(ListBuilder* builder, uint64_t value);
int list_builder_append_float32(ListBuilder* builder, float value);
int list_builder_append_float64(ListBuilder* builder, double value);
int list_builder_append_string(ListBuilder* builder, const char* value);
int list_builder_append_bool(ListBuilder* builder, bool value);
int list_builder_append_binary(ListBuilder* builder, const uint8_t* data, size_t length);

// Finish and get Arrow array
struct ArrowArray* list_builder_finish(ListBuilder* builder);

// Get the child schema format string
const char* list_builder_child_format(ListBuilder* builder);

// Free the builder
void list_builder_free(ListBuilder* builder);

// Get current list count
size_t list_builder_length(const ListBuilder* builder);

// ============================================================================
// Struct Builder
// ============================================================================

typedef struct {
    void** field_builders;      // Array of typed builders
    char** field_names;         // Field names
    char** field_formats;       // Arrow format strings for each field
    ListElementType* field_types; // Type of each field builder
    uint8_t* validity;          // Null bitmap for structs
    size_t num_fields;
    size_t length;              // Number of structs
    size_t capacity;
    size_t null_count;
} StructBuilder;

// Create a struct builder
StructBuilder* struct_builder_create(size_t initial_capacity);

// Add a field to the struct (must be called before appending values)
int struct_builder_add_field(StructBuilder* builder, const char* name, ListElementType type);

// Append values to a specific field
int struct_builder_append_int8(StructBuilder* builder, size_t field_idx, int8_t value);
int struct_builder_append_int16(StructBuilder* builder, size_t field_idx, int16_t value);
int struct_builder_append_int32(StructBuilder* builder, size_t field_idx, int32_t value);
int struct_builder_append_int64(StructBuilder* builder, size_t field_idx, int64_t value);
int struct_builder_append_uint8(StructBuilder* builder, size_t field_idx, uint8_t value);
int struct_builder_append_uint16(StructBuilder* builder, size_t field_idx, uint16_t value);
int struct_builder_append_uint32(StructBuilder* builder, size_t field_idx, uint32_t value);
int struct_builder_append_uint64(StructBuilder* builder, size_t field_idx, uint64_t value);
int struct_builder_append_float32(StructBuilder* builder, size_t field_idx, float value);
int struct_builder_append_float64(StructBuilder* builder, size_t field_idx, double value);
int struct_builder_append_string(StructBuilder* builder, size_t field_idx, const char* value);
int struct_builder_append_bool(StructBuilder* builder, size_t field_idx, bool value);
int struct_builder_append_binary(StructBuilder* builder, size_t field_idx, const uint8_t* data, size_t length);

// Append null to a specific field
int struct_builder_append_field_null(StructBuilder* builder, size_t field_idx);

// Append a null struct (all fields null)
int struct_builder_append_null(StructBuilder* builder);

// Finish a row (advance to next struct)
int struct_builder_finish_row(StructBuilder* builder);

// Finish and get Arrow array
struct ArrowArray* struct_builder_finish(StructBuilder* builder);

// Get the Arrow schema for this struct
struct ArrowSchema* struct_builder_get_schema(StructBuilder* builder);

// Free the builder
void struct_builder_free(StructBuilder* builder);

// Get current struct count
size_t struct_builder_length(const StructBuilder* builder);

// Get field count
size_t struct_builder_field_count(const StructBuilder* builder);

// ============================================================================
// Decimal128 Builder
// ============================================================================

typedef struct {
    uint8_t* values;            // 16 bytes per value, little-endian
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
    int32_t precision;          // 1-38
    int32_t scale;              // Number of digits after decimal point
} Decimal128Builder;

// Create a decimal128 builder
Decimal128Builder* decimal128_builder_create(size_t initial_capacity, int32_t precision, int32_t scale);

// Append a decimal value from high/low int64 parts
int decimal128_builder_append(Decimal128Builder* builder, int64_t high, uint64_t low);

// Append a decimal value from string (e.g., "123.45")
int decimal128_builder_append_string(Decimal128Builder* builder, const char* value);

// Append null
int decimal128_builder_append_null(Decimal128Builder* builder);

// Finish and get Arrow array
struct ArrowArray* decimal128_builder_finish(Decimal128Builder* builder);

// Free the builder
void decimal128_builder_free(Decimal128Builder* builder);

// Get length
size_t decimal128_builder_length(const Decimal128Builder* builder);

// Get precision
int32_t decimal128_builder_precision(const Decimal128Builder* builder);

// Get scale
int32_t decimal128_builder_scale(const Decimal128Builder* builder);

// ============================================================================
// Dictionary Builder (for string dictionaries)
// ============================================================================

typedef struct {
    int32_t* indices;           // Index into dictionary
    uint8_t* validity;          // Null bitmap
    StringBuilder* dictionary;  // The dictionary values
    // Simple hash map for deduplication
    char** hash_keys;
    int32_t* hash_values;
    size_t hash_capacity;
    size_t length;
    size_t capacity;
    size_t null_count;
} DictionaryBuilder;

// Create a dictionary builder for strings
DictionaryBuilder* dictionary_builder_create(size_t initial_capacity, size_t dict_capacity);

// Append a string value (will be deduplicated)
int dictionary_builder_append(DictionaryBuilder* builder, const char* value);

// Append null
int dictionary_builder_append_null(DictionaryBuilder* builder);

// Finish and get Arrow array (with dictionary)
struct ArrowArray* dictionary_builder_finish(DictionaryBuilder* builder, struct ArrowArray** out_dictionary);

// Free the builder
void dictionary_builder_free(DictionaryBuilder* builder);

// Get length
size_t dictionary_builder_length(const DictionaryBuilder* builder);

// Get dictionary size (unique values)
size_t dictionary_builder_dict_size(const DictionaryBuilder* builder);

// ============================================================================
// Map Builder (List of Struct<key, value>)
// ============================================================================

typedef struct {
    ListBuilder* list_builder;  // Underlying list builder (of structs)
    StructBuilder* entry_builder; // Builder for key-value pairs
    ListElementType key_type;
    ListElementType value_type;
} MapBuilder;

// Create a map builder
MapBuilder* map_builder_create(size_t initial_capacity, ListElementType key_type, ListElementType value_type);

// Start a new map entry
int map_builder_start_map(MapBuilder* builder);

// Finish current map entry
int map_builder_finish_map(MapBuilder* builder);

// Append a null map
int map_builder_append_null(MapBuilder* builder);

// Append key-value pairs (various type combinations)
int map_builder_append_string_int64(MapBuilder* builder, const char* key, int64_t value);
int map_builder_append_string_string(MapBuilder* builder, const char* key, const char* value);
int map_builder_append_string_float64(MapBuilder* builder, const char* key, double value);
int map_builder_append_int64_int64(MapBuilder* builder, int64_t key, int64_t value);
int map_builder_append_int64_string(MapBuilder* builder, int64_t key, const char* value);

// Finish and get Arrow array
struct ArrowArray* map_builder_finish(MapBuilder* builder);

// Free the builder
void map_builder_free(MapBuilder* builder);

// Get length
size_t map_builder_length(const MapBuilder* builder);

#ifdef __cplusplus
}
#endif

#endif // ARROW_NESTED_BUILDERS_H
