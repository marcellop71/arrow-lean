#ifndef PARQUET_WRITER_IMPL_H
#define PARQUET_WRITER_IMPL_H

#include "arrow_c_abi.h"
#include "arrow_builders.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Parquet Constants
// ============================================================================

#define PARQUET_MAGIC "PAR1"
#define PARQUET_MAGIC_SIZE 4
#define PARQUET_VERSION 2

// Parquet physical types
typedef enum {
    PARQUET_TYPE_BOOLEAN = 0,
    PARQUET_TYPE_INT32 = 1,
    PARQUET_TYPE_INT64 = 2,
    PARQUET_TYPE_INT96 = 3,  // Deprecated timestamp
    PARQUET_TYPE_FLOAT = 4,
    PARQUET_TYPE_DOUBLE = 5,
    PARQUET_TYPE_BYTE_ARRAY = 6,
    PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY = 7
} ParquetType;

// Parquet converted types (logical types)
typedef enum {
    PARQUET_CONVERTED_NONE = -1,
    PARQUET_CONVERTED_UTF8 = 0,
    PARQUET_CONVERTED_MAP = 1,
    PARQUET_CONVERTED_MAP_KEY_VALUE = 2,
    PARQUET_CONVERTED_LIST = 3,
    PARQUET_CONVERTED_ENUM = 4,
    PARQUET_CONVERTED_DECIMAL = 5,
    PARQUET_CONVERTED_DATE = 6,
    PARQUET_CONVERTED_TIME_MILLIS = 7,
    PARQUET_CONVERTED_TIME_MICROS = 8,
    PARQUET_CONVERTED_TIMESTAMP_MILLIS = 9,
    PARQUET_CONVERTED_TIMESTAMP_MICROS = 10,
    PARQUET_CONVERTED_UINT_8 = 11,
    PARQUET_CONVERTED_UINT_16 = 12,
    PARQUET_CONVERTED_UINT_32 = 13,
    PARQUET_CONVERTED_UINT_64 = 14,
    PARQUET_CONVERTED_INT_8 = 15,
    PARQUET_CONVERTED_INT_16 = 16,
    PARQUET_CONVERTED_INT_32 = 17,
    PARQUET_CONVERTED_INT_64 = 18,
    PARQUET_CONVERTED_JSON = 19,
    PARQUET_CONVERTED_BSON = 20,
    PARQUET_CONVERTED_INTERVAL = 21
} ParquetConvertedType;

// Parquet repetition types
typedef enum {
    PARQUET_REPETITION_REQUIRED = 0,
    PARQUET_REPETITION_OPTIONAL = 1,
    PARQUET_REPETITION_REPEATED = 2
} ParquetRepetition;

// Parquet encodings
typedef enum {
    PARQUET_ENCODING_PLAIN = 0,
    PARQUET_ENCODING_PLAIN_DICTIONARY = 2,
    PARQUET_ENCODING_RLE = 3,
    PARQUET_ENCODING_BIT_PACKED = 4,
    PARQUET_ENCODING_DELTA_BINARY_PACKED = 5,
    PARQUET_ENCODING_DELTA_LENGTH_BYTE_ARRAY = 6,
    PARQUET_ENCODING_DELTA_BYTE_ARRAY = 7,
    PARQUET_ENCODING_RLE_DICTIONARY = 8,
    PARQUET_ENCODING_BYTE_STREAM_SPLIT = 9
} ParquetEncoding;

// Parquet compression codecs
typedef enum {
    PARQUET_CODEC_UNCOMPRESSED = 0,
    PARQUET_CODEC_SNAPPY = 1,
    PARQUET_CODEC_GZIP = 2,
    PARQUET_CODEC_LZO = 3,
    PARQUET_CODEC_BROTLI = 4,
    PARQUET_CODEC_LZ4 = 5,
    PARQUET_CODEC_ZSTD = 6,
    PARQUET_CODEC_LZ4_RAW = 7
} ParquetCompressionCodec;

// Page types
typedef enum {
    PARQUET_PAGE_DATA = 0,
    PARQUET_PAGE_INDEX = 1,
    PARQUET_PAGE_DICTIONARY = 2,
    PARQUET_PAGE_DATA_V2 = 3
} ParquetPageType;

// ============================================================================
// Writer Structures
// ============================================================================

// Column metadata for writing
typedef struct {
    char* name;
    ParquetType type;
    ParquetConvertedType converted_type;
    ParquetRepetition repetition;
    int32_t type_length;  // For fixed-length types
} ParquetColumnDef;

// Column chunk statistics
typedef struct {
    bool has_min_max;
    int64_t min_int64;
    int64_t max_int64;
    double min_double;
    double max_double;
    int64_t null_count;
    int64_t distinct_count;
} ParquetColumnStats;

// Column chunk info (after writing)
typedef struct {
    int64_t file_offset;
    int64_t total_compressed_size;
    int64_t total_uncompressed_size;
    int64_t num_values;
    int64_t data_page_offset;
    int64_t dictionary_page_offset;
    ParquetEncoding* encodings;
    int num_encodings;
    ParquetColumnStats stats;
} ParquetColumnChunkInfo;

// Row group info
typedef struct {
    int64_t num_rows;
    int64_t total_byte_size;
    ParquetColumnChunkInfo* columns;
    int num_columns;
} ParquetRowGroupInfo;

// Parquet file writer state
typedef struct {
    FILE* file;
    char* file_path;

    // Schema
    ParquetColumnDef* columns;
    int num_columns;

    // Row groups
    ParquetRowGroupInfo* row_groups;
    int num_row_groups;
    int row_groups_capacity;

    // Current state
    int64_t current_offset;
    ParquetCompressionCodec compression;

    // Options
    int64_t row_group_size;  // Max bytes per row group
    bool write_statistics;

    // Created by info
    char* created_by;
} ParquetFileWriter;

// ============================================================================
// Thrift Compact Protocol Writer
// ============================================================================

// Buffer for building Thrift-encoded data
typedef struct {
    uint8_t* data;
    size_t size;
    size_t capacity;
} ThriftBuffer;

ThriftBuffer* thrift_buffer_create(size_t initial_capacity);
void thrift_buffer_free(ThriftBuffer* buf);
int thrift_buffer_ensure_capacity(ThriftBuffer* buf, size_t additional);
int thrift_buffer_write_byte(ThriftBuffer* buf, uint8_t value);
int thrift_buffer_write_bytes(ThriftBuffer* buf, const void* data, size_t len);
int thrift_buffer_write_varint(ThriftBuffer* buf, uint64_t value);
int thrift_buffer_write_zigzag(ThriftBuffer* buf, int64_t value);
int thrift_buffer_write_string(ThriftBuffer* buf, const char* str);
int thrift_buffer_write_binary(ThriftBuffer* buf, const void* data, size_t len);

// ============================================================================
// Parquet Writer API
// ============================================================================

// Create a new Parquet writer
ParquetFileWriter* parquet_file_writer_create(const char* path);

// Set compression codec
void parquet_file_writer_set_compression(ParquetFileWriter* writer, ParquetCompressionCodec codec);

// Set row group size (bytes)
void parquet_file_writer_set_row_group_size(ParquetFileWriter* writer, int64_t size);

// Add a column to the schema
int parquet_file_writer_add_column(
    ParquetFileWriter* writer,
    const char* name,
    ParquetType type,
    ParquetConvertedType converted_type,
    ParquetRepetition repetition
);

// Write schema from Arrow schema
int parquet_file_writer_set_schema_from_arrow(
    ParquetFileWriter* writer,
    struct ArrowSchema* schema
);

// Write a record batch
int parquet_file_writer_write_batch(
    ParquetFileWriter* writer,
    struct ArrowArray* array,
    struct ArrowSchema* schema
);

// Close the writer (writes footer and closes file)
int parquet_file_writer_close(ParquetFileWriter* writer);

// Free the writer resources
void parquet_file_writer_free(ParquetFileWriter* writer);

// ============================================================================
// High-level API (integrates with existing parquet_wrapper.h)
// ============================================================================

// Write Arrow data to Parquet file
int write_arrow_to_parquet(
    const char* path,
    struct ArrowSchema* schema,
    struct ArrowArray* array,
    ParquetCompressionCodec compression
);

// Write Arrow stream to Parquet file
int write_arrow_stream_to_parquet(
    const char* path,
    struct ArrowArrayStream* stream,
    ParquetCompressionCodec compression
);

#ifdef __cplusplus
}
#endif

#endif // PARQUET_WRITER_IMPL_H
