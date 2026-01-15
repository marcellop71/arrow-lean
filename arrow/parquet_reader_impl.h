#ifndef PARQUET_READER_IMPL_H
#define PARQUET_READER_IMPL_H

#include "arrow_c_abi.h"
#include "parquet_writer_impl.h"  // Reuse type definitions
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Thrift Compact Protocol Reader
// ============================================================================

typedef struct {
    const uint8_t* data;
    size_t size;
    size_t pos;
} ThriftReader;

// Initialize a Thrift reader
void thrift_reader_init(ThriftReader* reader, const uint8_t* data, size_t size);

// Read a single byte
int thrift_reader_read_byte(ThriftReader* reader, uint8_t* out);

// Read multiple bytes
int thrift_reader_read_bytes(ThriftReader* reader, void* out, size_t len);

// Read unsigned varint
int thrift_reader_read_varint(ThriftReader* reader, uint64_t* out);

// Read zigzag-encoded signed integer
int thrift_reader_read_zigzag(ThriftReader* reader, int64_t* out);

// Read a string (length-prefixed)
int thrift_reader_read_string(ThriftReader* reader, char** out);

// Read binary data (length-prefixed)
int thrift_reader_read_binary(ThriftReader* reader, uint8_t** out, size_t* out_len);

// Read field header (returns field_id and type)
int thrift_reader_read_field_header(ThriftReader* reader, int16_t* field_id,
                                     uint8_t* type, int16_t last_field_id);

// Read list header (returns element type and count)
int thrift_reader_read_list_header(ThriftReader* reader, uint8_t* elem_type, size_t* count);

// Skip a field value of given type
int thrift_reader_skip_field(ThriftReader* reader, uint8_t type);

// ============================================================================
// Parquet File Metadata (read from footer)
// ============================================================================

typedef struct {
    char* name;
    ParquetType type;
    ParquetConvertedType converted_type;
    ParquetRepetition repetition;
    int32_t num_children;
    int32_t type_length;
    int32_t precision;
    int32_t scale;
} ParquetSchemaElement;

typedef struct {
    int64_t file_offset;
    int64_t total_compressed_size;
    int64_t total_uncompressed_size;
    int64_t num_values;
    int64_t data_page_offset;
    int64_t dictionary_page_offset;
    ParquetCompressionCodec codec;
    ParquetEncoding* encodings;
    int num_encodings;
    ParquetType type;
    char* path_in_schema;
} ParquetColumnChunkMeta;

typedef struct {
    int64_t num_rows;
    int64_t total_byte_size;
    int64_t file_offset;
    ParquetColumnChunkMeta* columns;
    int num_columns;
} ParquetRowGroupMeta;

typedef struct {
    int32_t version;
    int64_t num_rows;
    char* created_by;

    // Schema
    ParquetSchemaElement* schema;
    int num_schema_elements;

    // Row groups
    ParquetRowGroupMeta* row_groups;
    int num_row_groups;

    // Key-value metadata
    char** kv_keys;
    char** kv_values;
    int num_kv;
} ParquetFileMeta;

// Free metadata structures
void parquet_schema_element_free(ParquetSchemaElement* elem);
void parquet_column_chunk_meta_free(ParquetColumnChunkMeta* chunk);
void parquet_row_group_meta_free(ParquetRowGroupMeta* rg);
void parquet_file_meta_free(ParquetFileMeta* meta);

// ============================================================================
// Page Header (read before each data page)
// ============================================================================

typedef struct {
    ParquetPageType type;
    int32_t uncompressed_page_size;
    int32_t compressed_page_size;
    int32_t crc;  // Optional

    // For data pages
    int32_t num_values;
    ParquetEncoding encoding;
    ParquetEncoding definition_level_encoding;
    ParquetEncoding repetition_level_encoding;

    // For data page v2
    int32_t num_nulls;
    int32_t num_rows;
    bool is_compressed;

    // For dictionary pages
    int32_t num_dict_values;
    ParquetEncoding dict_encoding;
} ParquetPageHeader;

void parquet_page_header_init(ParquetPageHeader* header);

// ============================================================================
// RLE/Bit-Packing Decoder (for definition/repetition levels)
// ============================================================================

typedef struct {
    const uint8_t* data;
    size_t size;
    size_t pos;
    int bit_width;

    // Current run state
    int current_value;
    int remaining_in_run;
    bool is_literal_run;

    // Bit buffer for literal runs
    uint64_t bit_buffer;
    int bits_in_buffer;
} RleDecoder;

// Initialize RLE decoder
void rle_decoder_init(RleDecoder* decoder, const uint8_t* data, size_t size, int bit_width);

// Get next value (returns 0 on success, -1 on error/end)
int rle_decoder_next(RleDecoder* decoder, int32_t* out);

// Decode multiple values
int rle_decoder_decode_batch(RleDecoder* decoder, int32_t* out, int count);

// ============================================================================
// Plain Encoding Decoders
// ============================================================================

// Decode plain-encoded boolean values (bit-packed)
int decode_plain_boolean(const uint8_t* data, size_t size, bool* out, int count);

// Decode plain-encoded int32 values
int decode_plain_int32(const uint8_t* data, size_t size, int32_t* out, int count);

// Decode plain-encoded int64 values
int decode_plain_int64(const uint8_t* data, size_t size, int64_t* out, int count);

// Decode plain-encoded float values
int decode_plain_float(const uint8_t* data, size_t size, float* out, int count);

// Decode plain-encoded double values
int decode_plain_double(const uint8_t* data, size_t size, double* out, int count);

// Decode plain-encoded byte array values (variable length)
int decode_plain_byte_array(const uint8_t* data, size_t size,
                            int32_t** offsets, uint8_t** values,
                            int count, size_t* values_len);

// ============================================================================
// Parquet File Reader
// ============================================================================

typedef struct {
    FILE* file;
    char* file_path;
    int64_t file_size;

    // Parsed metadata
    ParquetFileMeta* metadata;

    // Current reading state
    int current_row_group;
    int current_column;

    // Buffers
    uint8_t* page_buffer;
    size_t page_buffer_size;
    size_t page_buffer_capacity;

    uint8_t* decompress_buffer;
    size_t decompress_buffer_capacity;
} ParquetFileReader;

// Open a Parquet file for reading
ParquetFileReader* parquet_file_reader_open(const char* path);

// Get file metadata
const ParquetFileMeta* parquet_file_reader_get_metadata(ParquetFileReader* reader);

// Get number of row groups
int parquet_file_reader_num_row_groups(ParquetFileReader* reader);

// Get row group metadata
const ParquetRowGroupMeta* parquet_file_reader_get_row_group(ParquetFileReader* reader, int index);

// Read a row group into Arrow arrays
// Returns an ArrowArrayStream with the data
struct ArrowArrayStream* parquet_file_reader_read_row_group(ParquetFileReader* reader, int row_group_index);

// Read specific columns from a row group
struct ArrowArrayStream* parquet_file_reader_read_columns(ParquetFileReader* reader,
                                                           int row_group_index,
                                                           int* column_indices,
                                                           int num_columns);

// Read the entire file
struct ArrowArrayStream* parquet_file_reader_read_all(ParquetFileReader* reader);

// Close and free the reader
void parquet_file_reader_close(ParquetFileReader* reader);

// ============================================================================
// Internal Functions (exposed for testing)
// ============================================================================

// Read and parse the footer metadata
int parquet_read_footer(FILE* file, int64_t file_size, ParquetFileMeta** out_meta);

// Parse a page header from Thrift data
int parquet_parse_page_header(ThriftReader* reader, ParquetPageHeader* out);

// Parse FileMetaData from Thrift data
int parquet_parse_file_metadata(ThriftReader* reader, ParquetFileMeta* out);

// Read and decode a data page into Arrow array
int parquet_read_data_page(ParquetFileReader* reader,
                           ParquetColumnChunkMeta* column_meta,
                           struct ArrowArray* out_array,
                           struct ArrowSchema* schema);

// Create Arrow schema from Parquet schema
int parquet_schema_to_arrow(const ParquetFileMeta* meta, struct ArrowSchema* out);

#ifdef __cplusplus
}
#endif

#endif // PARQUET_READER_IMPL_H
