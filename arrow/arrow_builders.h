#ifndef ARROW_BUILDERS_H
#define ARROW_BUILDERS_H

#include "arrow_c_abi.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Builder Return Codes
// ============================================================================

#define BUILDER_OK          0
#define BUILDER_ERR_ALLOC  -1
#define BUILDER_ERR_NULL   -2
#define BUILDER_ERR_FULL   -3

// ============================================================================
// Builder Structures
// ============================================================================

// Int8 Builder
typedef struct {
    int8_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} Int8Builder;

// Int16 Builder
typedef struct {
    int16_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} Int16Builder;

// Int32 Builder
typedef struct {
    int32_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} Int32Builder;

// Int64 Builder
typedef struct {
    int64_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} Int64Builder;

// UInt8 Builder
typedef struct {
    uint8_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} UInt8Builder;

// UInt16 Builder
typedef struct {
    uint16_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} UInt16Builder;

// UInt32 Builder
typedef struct {
    uint32_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} UInt32Builder;

// UInt64 Builder
typedef struct {
    uint64_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} UInt64Builder;

// Float32 Builder
typedef struct {
    float* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} Float32Builder;

// Float64 Builder
typedef struct {
    double* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} Float64Builder;

// String Builder (variable-length, uses offset buffer)
typedef struct {
    int32_t* offsets;       // Offset buffer (length + 1 elements)
    char* data;             // Character data buffer
    uint8_t* validity;      // Null bitmap
    size_t length;          // Number of strings
    size_t capacity;        // Capacity for strings (offsets)
    size_t data_length;     // Current bytes used in data buffer
    size_t data_capacity;   // Capacity of data buffer
    size_t null_count;
} StringBuilder;

// Timestamp Builder (int64 microseconds since epoch)
typedef struct {
    int64_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
    char* timezone;         // e.g., "UTC", "America/New_York"
} TimestampBuilder;

// Bool Builder
typedef struct {
    uint8_t* values;        // Bit-packed boolean values
    uint8_t* validity;      // Null bitmap
    size_t length;
    size_t capacity;
    size_t null_count;
} BoolBuilder;

// Date32 Builder (days since epoch)
typedef struct {
    int32_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} Date32Builder;

// Date64 Builder (milliseconds since epoch)
typedef struct {
    int64_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
} Date64Builder;

// Time32 Builder (seconds or milliseconds)
typedef struct {
    int32_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
    char unit;  // 's' for seconds, 'm' for milliseconds
} Time32Builder;

// Time64 Builder (microseconds or nanoseconds)
typedef struct {
    int64_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
    char unit;  // 'u' for microseconds, 'n' for nanoseconds
} Time64Builder;

// Duration Builder (int64 with time unit)
typedef struct {
    int64_t* values;
    uint8_t* validity;
    size_t length;
    size_t capacity;
    size_t null_count;
    char unit;  // 's', 'm', 'u', 'n' for seconds, milliseconds, microseconds, nanoseconds
} DurationBuilder;

// Binary Builder (variable-length binary, like StringBuilder)
typedef struct {
    int32_t* offsets;       // Offset buffer (length + 1 elements)
    uint8_t* data;          // Binary data buffer
    uint8_t* validity;      // Null bitmap
    size_t length;          // Number of binary values
    size_t capacity;        // Capacity for values (offsets)
    size_t data_length;     // Current bytes used in data buffer
    size_t data_capacity;   // Capacity of data buffer
    size_t null_count;
} BinaryBuilder;

// ============================================================================
// Int8 Builder Functions
// ============================================================================

Int8Builder* int8_builder_create(size_t initial_capacity);
int int8_builder_append(Int8Builder* builder, int8_t value);
int int8_builder_append_null(Int8Builder* builder);
struct ArrowArray* int8_builder_finish(Int8Builder* builder);
void int8_builder_reset(Int8Builder* builder);
void int8_builder_free(Int8Builder* builder);
size_t int8_builder_length(const Int8Builder* builder);

// ============================================================================
// Int16 Builder Functions
// ============================================================================

Int16Builder* int16_builder_create(size_t initial_capacity);
int int16_builder_append(Int16Builder* builder, int16_t value);
int int16_builder_append_null(Int16Builder* builder);
struct ArrowArray* int16_builder_finish(Int16Builder* builder);
void int16_builder_reset(Int16Builder* builder);
void int16_builder_free(Int16Builder* builder);
size_t int16_builder_length(const Int16Builder* builder);

// ============================================================================
// Int32 Builder Functions
// ============================================================================

Int32Builder* int32_builder_create(size_t initial_capacity);
int int32_builder_append(Int32Builder* builder, int32_t value);
int int32_builder_append_null(Int32Builder* builder);
struct ArrowArray* int32_builder_finish(Int32Builder* builder);
void int32_builder_reset(Int32Builder* builder);
void int32_builder_free(Int32Builder* builder);
size_t int32_builder_length(const Int32Builder* builder);

// ============================================================================
// Int64 Builder Functions
// ============================================================================

Int64Builder* int64_builder_create(size_t initial_capacity);
int int64_builder_append(Int64Builder* builder, int64_t value);
int int64_builder_append_null(Int64Builder* builder);
int int64_builder_append_values(Int64Builder* builder, const int64_t* values, size_t count);
struct ArrowArray* int64_builder_finish(Int64Builder* builder);
void int64_builder_reset(Int64Builder* builder);
void int64_builder_free(Int64Builder* builder);
size_t int64_builder_length(const Int64Builder* builder);

// ============================================================================
// UInt8 Builder Functions
// ============================================================================

UInt8Builder* uint8_builder_create(size_t initial_capacity);
int uint8_builder_append(UInt8Builder* builder, uint8_t value);
int uint8_builder_append_null(UInt8Builder* builder);
struct ArrowArray* uint8_builder_finish(UInt8Builder* builder);
void uint8_builder_reset(UInt8Builder* builder);
void uint8_builder_free(UInt8Builder* builder);
size_t uint8_builder_length(const UInt8Builder* builder);

// ============================================================================
// UInt16 Builder Functions
// ============================================================================

UInt16Builder* uint16_builder_create(size_t initial_capacity);
int uint16_builder_append(UInt16Builder* builder, uint16_t value);
int uint16_builder_append_null(UInt16Builder* builder);
struct ArrowArray* uint16_builder_finish(UInt16Builder* builder);
void uint16_builder_reset(UInt16Builder* builder);
void uint16_builder_free(UInt16Builder* builder);
size_t uint16_builder_length(const UInt16Builder* builder);

// ============================================================================
// UInt32 Builder Functions
// ============================================================================

UInt32Builder* uint32_builder_create(size_t initial_capacity);
int uint32_builder_append(UInt32Builder* builder, uint32_t value);
int uint32_builder_append_null(UInt32Builder* builder);
struct ArrowArray* uint32_builder_finish(UInt32Builder* builder);
void uint32_builder_reset(UInt32Builder* builder);
void uint32_builder_free(UInt32Builder* builder);
size_t uint32_builder_length(const UInt32Builder* builder);

// ============================================================================
// UInt64 Builder Functions
// ============================================================================

UInt64Builder* uint64_builder_create(size_t initial_capacity);
int uint64_builder_append(UInt64Builder* builder, uint64_t value);
int uint64_builder_append_null(UInt64Builder* builder);
struct ArrowArray* uint64_builder_finish(UInt64Builder* builder);
void uint64_builder_reset(UInt64Builder* builder);
void uint64_builder_free(UInt64Builder* builder);
size_t uint64_builder_length(const UInt64Builder* builder);

// ============================================================================
// Float32 Builder Functions
// ============================================================================

Float32Builder* float32_builder_create(size_t initial_capacity);
int float32_builder_append(Float32Builder* builder, float value);
int float32_builder_append_null(Float32Builder* builder);
struct ArrowArray* float32_builder_finish(Float32Builder* builder);
void float32_builder_reset(Float32Builder* builder);
void float32_builder_free(Float32Builder* builder);
size_t float32_builder_length(const Float32Builder* builder);

// ============================================================================
// Float64 Builder Functions
// ============================================================================

Float64Builder* float64_builder_create(size_t initial_capacity);
int float64_builder_append(Float64Builder* builder, double value);
int float64_builder_append_null(Float64Builder* builder);
int float64_builder_append_values(Float64Builder* builder, const double* values, size_t count);
struct ArrowArray* float64_builder_finish(Float64Builder* builder);
void float64_builder_reset(Float64Builder* builder);
void float64_builder_free(Float64Builder* builder);
size_t float64_builder_length(const Float64Builder* builder);

// ============================================================================
// String Builder Functions
// ============================================================================

StringBuilder* string_builder_create(size_t initial_capacity, size_t initial_data_capacity);
int string_builder_append(StringBuilder* builder, const char* value, size_t length);
int string_builder_append_cstr(StringBuilder* builder, const char* value);
int string_builder_append_null(StringBuilder* builder);
struct ArrowArray* string_builder_finish(StringBuilder* builder);
void string_builder_reset(StringBuilder* builder);
void string_builder_free(StringBuilder* builder);
size_t string_builder_length(const StringBuilder* builder);

// ============================================================================
// Timestamp Builder Functions
// ============================================================================

TimestampBuilder* timestamp_builder_create(size_t initial_capacity, const char* timezone);
int timestamp_builder_append(TimestampBuilder* builder, int64_t microseconds);
int timestamp_builder_append_null(TimestampBuilder* builder);
struct ArrowArray* timestamp_builder_finish(TimestampBuilder* builder);
void timestamp_builder_reset(TimestampBuilder* builder);
void timestamp_builder_free(TimestampBuilder* builder);
size_t timestamp_builder_length(const TimestampBuilder* builder);
const char* timestamp_builder_get_timezone(const TimestampBuilder* builder);

// ============================================================================
// Bool Builder Functions
// ============================================================================

BoolBuilder* bool_builder_create(size_t initial_capacity);
int bool_builder_append(BoolBuilder* builder, bool value);
int bool_builder_append_null(BoolBuilder* builder);
struct ArrowArray* bool_builder_finish(BoolBuilder* builder);
void bool_builder_reset(BoolBuilder* builder);
void bool_builder_free(BoolBuilder* builder);
size_t bool_builder_length(const BoolBuilder* builder);

// ============================================================================
// Date32 Builder Functions
// ============================================================================

Date32Builder* date32_builder_create(size_t initial_capacity);
int date32_builder_append(Date32Builder* builder, int32_t days);
int date32_builder_append_null(Date32Builder* builder);
struct ArrowArray* date32_builder_finish(Date32Builder* builder);
void date32_builder_reset(Date32Builder* builder);
void date32_builder_free(Date32Builder* builder);
size_t date32_builder_length(const Date32Builder* builder);

// ============================================================================
// Date64 Builder Functions
// ============================================================================

Date64Builder* date64_builder_create(size_t initial_capacity);
int date64_builder_append(Date64Builder* builder, int64_t milliseconds);
int date64_builder_append_null(Date64Builder* builder);
struct ArrowArray* date64_builder_finish(Date64Builder* builder);
void date64_builder_reset(Date64Builder* builder);
void date64_builder_free(Date64Builder* builder);
size_t date64_builder_length(const Date64Builder* builder);

// ============================================================================
// Time32 Builder Functions
// ============================================================================

Time32Builder* time32_builder_create(size_t initial_capacity, char unit);  // 's' or 'm'
int time32_builder_append(Time32Builder* builder, int32_t value);
int time32_builder_append_null(Time32Builder* builder);
struct ArrowArray* time32_builder_finish(Time32Builder* builder);
void time32_builder_reset(Time32Builder* builder);
void time32_builder_free(Time32Builder* builder);
size_t time32_builder_length(const Time32Builder* builder);

// ============================================================================
// Time64 Builder Functions
// ============================================================================

Time64Builder* time64_builder_create(size_t initial_capacity, char unit);  // 'u' or 'n'
int time64_builder_append(Time64Builder* builder, int64_t value);
int time64_builder_append_null(Time64Builder* builder);
struct ArrowArray* time64_builder_finish(Time64Builder* builder);
void time64_builder_reset(Time64Builder* builder);
void time64_builder_free(Time64Builder* builder);
size_t time64_builder_length(const Time64Builder* builder);

// ============================================================================
// Duration Builder Functions
// ============================================================================

DurationBuilder* duration_builder_create(size_t initial_capacity, char unit);  // 's', 'm', 'u', 'n'
int duration_builder_append(DurationBuilder* builder, int64_t value);
int duration_builder_append_null(DurationBuilder* builder);
struct ArrowArray* duration_builder_finish(DurationBuilder* builder);
void duration_builder_reset(DurationBuilder* builder);
void duration_builder_free(DurationBuilder* builder);
size_t duration_builder_length(const DurationBuilder* builder);

// ============================================================================
// Binary Builder Functions
// ============================================================================

BinaryBuilder* binary_builder_create(size_t initial_capacity, size_t initial_data_capacity);
int binary_builder_append(BinaryBuilder* builder, const uint8_t* value, size_t length);
int binary_builder_append_null(BinaryBuilder* builder);
struct ArrowArray* binary_builder_finish(BinaryBuilder* builder);
void binary_builder_reset(BinaryBuilder* builder);
void binary_builder_free(BinaryBuilder* builder);
size_t binary_builder_length(const BinaryBuilder* builder);

// ============================================================================
// Schema Builder
// ============================================================================

typedef struct {
    char** names;
    char** formats;
    int64_t* flags;
    size_t count;
    size_t capacity;
} SchemaBuilder;

SchemaBuilder* schema_builder_create(size_t initial_capacity);
int schema_builder_add_field(SchemaBuilder* builder, const char* name, const char* format, int64_t flags);
int schema_builder_add_int64(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_float64(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_string(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_timestamp_us(SchemaBuilder* builder, const char* name, const char* timezone, bool nullable);
int schema_builder_add_bool(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_int8(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_int16(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_int32(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_uint8(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_uint16(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_uint32(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_uint64(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_float32(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_date32(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_date64(SchemaBuilder* builder, const char* name, bool nullable);
int schema_builder_add_time32(SchemaBuilder* builder, const char* name, char unit, bool nullable);
int schema_builder_add_time64(SchemaBuilder* builder, const char* name, char unit, bool nullable);
int schema_builder_add_duration(SchemaBuilder* builder, const char* name, char unit, bool nullable);
int schema_builder_add_binary(SchemaBuilder* builder, const char* name, bool nullable);
struct ArrowSchema* schema_builder_finish(SchemaBuilder* builder);
void schema_builder_reset(SchemaBuilder* builder);
void schema_builder_free(SchemaBuilder* builder);
size_t schema_builder_field_count(const SchemaBuilder* builder);

// ============================================================================
// RecordBatch
// ============================================================================

typedef struct {
    struct ArrowSchema* schema;     // Owned schema
    struct ArrowArray** columns;    // Array of column arrays (owned)
    size_t num_columns;
    size_t num_rows;
} RecordBatch;

// Create a record batch from schema and column arrays
// Takes ownership of schema and columns
RecordBatch* record_batch_create(
    struct ArrowSchema* schema,
    struct ArrowArray** columns,
    size_t num_columns,
    size_t num_rows
);

// Get the schema (borrowed reference)
struct ArrowSchema* record_batch_get_schema(const RecordBatch* batch);

// Get a column by index (borrowed reference)
struct ArrowArray* record_batch_get_column(const RecordBatch* batch, size_t index);

// Get number of rows
size_t record_batch_num_rows(const RecordBatch* batch);

// Get number of columns
size_t record_batch_num_columns(const RecordBatch* batch);

// Convert record batch to a struct ArrowArray (for Parquet writing)
// The returned array takes ownership of the batch's data
struct ArrowArray* record_batch_to_struct_array(RecordBatch* batch);

// Free record batch and all owned data
void record_batch_free(RecordBatch* batch);

// ============================================================================
// ArrowArrayStream from RecordBatches
// ============================================================================

// Create stream from a single batch (takes ownership of batch)
struct ArrowArrayStream* batch_to_stream(RecordBatch* batch);

// Create stream from multiple batches (takes ownership of batches array and its contents)
struct ArrowArrayStream* batches_to_stream(
    struct ArrowSchema* schema,
    RecordBatch** batches,
    size_t num_batches
);

// ============================================================================
// Utility Functions
// ============================================================================

// Calculate bytes needed for validity bitmap
static inline size_t bitmap_byte_count(size_t bits) {
    return (bits + 7) / 8;
}

// Set a bit in bitmap (1 = valid/not-null, 0 = null)
static inline void bitmap_set(uint8_t* bitmap, size_t index, bool value) {
    size_t byte_idx = index / 8;
    size_t bit_idx = index % 8;
    if (value) {
        bitmap[byte_idx] |= (uint8_t)(1 << bit_idx);
    } else {
        bitmap[byte_idx] &= (uint8_t)~(1 << bit_idx);
    }
}

// Get a bit from bitmap
static inline bool bitmap_get(const uint8_t* bitmap, size_t index) {
    size_t byte_idx = index / 8;
    size_t bit_idx = index % 8;
    return (bitmap[byte_idx] >> bit_idx) & 1;
}

#ifdef __cplusplus
}
#endif

#endif // ARROW_BUILDERS_H
