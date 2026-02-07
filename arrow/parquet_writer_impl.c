// Enable POSIX extensions
#define _POSIX_C_SOURCE 200809L

#include "parquet_writer_impl.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <zstd.h>

// ============================================================================
// Thrift Compact Protocol Implementation
// ============================================================================

// Compact protocol type IDs
#define THRIFT_CT_STOP          0
#define THRIFT_CT_BOOLEAN_TRUE  1
#define THRIFT_CT_BOOLEAN_FALSE 2
#define THRIFT_CT_BYTE          3
#define THRIFT_CT_I16           4
#define THRIFT_CT_I32           5
#define THRIFT_CT_I64           6
#define THRIFT_CT_DOUBLE        7
#define THRIFT_CT_BINARY        8
#define THRIFT_CT_LIST          9
#define THRIFT_CT_SET           10
#define THRIFT_CT_MAP           11
#define THRIFT_CT_STRUCT        12

ThriftBuffer* thrift_buffer_create(size_t initial_capacity) {
    ThriftBuffer* buf = malloc(sizeof(ThriftBuffer));
    if (!buf) return NULL;

    buf->data = malloc(initial_capacity);
    if (!buf->data) {
        free(buf);
        return NULL;
    }

    buf->size = 0;
    buf->capacity = initial_capacity;
    return buf;
}

void thrift_buffer_free(ThriftBuffer* buf) {
    if (!buf) return;
    free(buf->data);
    free(buf);
}

int thrift_buffer_ensure_capacity(ThriftBuffer* buf, size_t additional) {
    size_t required = buf->size + additional;
    if (required <= buf->capacity) return 0;

    size_t new_capacity = buf->capacity * 2;
    while (new_capacity < required) new_capacity *= 2;

    uint8_t* new_data = realloc(buf->data, new_capacity);
    if (!new_data) return -1;

    buf->data = new_data;
    buf->capacity = new_capacity;
    return 0;
}

int thrift_buffer_write_byte(ThriftBuffer* buf, uint8_t value) {
    if (thrift_buffer_ensure_capacity(buf, 1) != 0) return -1;
    buf->data[buf->size++] = value;
    return 0;
}

int thrift_buffer_write_bytes(ThriftBuffer* buf, const void* data, size_t len) {
    if (thrift_buffer_ensure_capacity(buf, len) != 0) return -1;
    memcpy(buf->data + buf->size, data, len);
    buf->size += len;
    return 0;
}

// Write unsigned varint (used for lengths, etc.)
int thrift_buffer_write_varint(ThriftBuffer* buf, uint64_t value) {
    while (value >= 0x80) {
        if (thrift_buffer_write_byte(buf, (uint8_t)(value | 0x80)) != 0) return -1;
        value >>= 7;
    }
    return thrift_buffer_write_byte(buf, (uint8_t)value);
}

// Write zigzag-encoded signed integer
int thrift_buffer_write_zigzag(ThriftBuffer* buf, int64_t value) {
    uint64_t zigzag = (uint64_t)((value << 1) ^ (value >> 63));
    return thrift_buffer_write_varint(buf, zigzag);
}

int thrift_buffer_write_string(ThriftBuffer* buf, const char* str) {
    size_t len = str ? strlen(str) : 0;
    if (thrift_buffer_write_varint(buf, len) != 0) return -1;
    if (len > 0) {
        return thrift_buffer_write_bytes(buf, str, len);
    }
    return 0;
}

int thrift_buffer_write_binary(ThriftBuffer* buf, const void* data, size_t len) {
    if (thrift_buffer_write_varint(buf, len) != 0) return -1;
    if (len > 0) {
        return thrift_buffer_write_bytes(buf, data, len);
    }
    return 0;
}

// Write a field header in compact protocol
static int thrift_write_field_header(ThriftBuffer* buf, int16_t field_id, uint8_t type, int16_t* last_field_id) {
    int16_t delta = field_id - *last_field_id;
    if (delta > 0 && delta <= 15) {
        // Short form: delta and type in one byte
        if (thrift_buffer_write_byte(buf, (uint8_t)((delta << 4) | type)) != 0) return -1;
    } else {
        // Long form: type byte, then zigzag field id
        if (thrift_buffer_write_byte(buf, type) != 0) return -1;
        if (thrift_buffer_write_zigzag(buf, field_id) != 0) return -1;
    }
    *last_field_id = field_id;
    return 0;
}

static int thrift_write_field_stop(ThriftBuffer* buf) {
    return thrift_buffer_write_byte(buf, THRIFT_CT_STOP);
}

static int thrift_write_i32(ThriftBuffer* buf, int16_t field_id, int32_t value, int16_t* last_field_id) {
    if (thrift_write_field_header(buf, field_id, THRIFT_CT_I32, last_field_id) != 0) return -1;
    return thrift_buffer_write_zigzag(buf, value);
}

static int thrift_write_i64(ThriftBuffer* buf, int16_t field_id, int64_t value, int16_t* last_field_id) {
    if (thrift_write_field_header(buf, field_id, THRIFT_CT_I64, last_field_id) != 0) return -1;
    return thrift_buffer_write_zigzag(buf, value);
}

static int thrift_write_string_field(ThriftBuffer* buf, int16_t field_id, const char* value, int16_t* last_field_id) {
    if (thrift_write_field_header(buf, field_id, THRIFT_CT_BINARY, last_field_id) != 0) return -1;
    return thrift_buffer_write_string(buf, value);
}

static int thrift_write_binary_field(ThriftBuffer* buf, int16_t field_id, const void* data, size_t len, int16_t* last_field_id) {
    if (thrift_write_field_header(buf, field_id, THRIFT_CT_BINARY, last_field_id) != 0) return -1;
    return thrift_buffer_write_binary(buf, data, len);
}

static int thrift_write_list_header(ThriftBuffer* buf, int16_t field_id, uint8_t elem_type, size_t count, int16_t* last_field_id) {
    if (thrift_write_field_header(buf, field_id, THRIFT_CT_LIST, last_field_id) != 0) return -1;
    if (count < 15) {
        if (thrift_buffer_write_byte(buf, (uint8_t)((count << 4) | elem_type)) != 0) return -1;
    } else {
        if (thrift_buffer_write_byte(buf, (uint8_t)(0xF0 | elem_type)) != 0) return -1;
        if (thrift_buffer_write_varint(buf, count) != 0) return -1;
    }
    return 0;
}

// ============================================================================
// Parquet Thrift Structures Serialization
// ============================================================================

// Serialize SchemaElement
static int serialize_schema_element(ThriftBuffer* buf, ParquetColumnDef* col, bool is_root, int32_t num_children) {
    int16_t last_field = 0;

    // Field 1: type (only for leaf nodes)
    if (!is_root && col->type >= 0) {
        if (thrift_write_i32(buf, 1, col->type, &last_field) != 0) return -1;
    }

    // Field 2: type_length (for fixed-length types)
    if (col->type_length > 0) {
        if (thrift_write_i32(buf, 2, col->type_length, &last_field) != 0) return -1;
    }

    // Field 3: repetition_type
    if (!is_root) {
        if (thrift_write_i32(buf, 3, col->repetition, &last_field) != 0) return -1;
    }

    // Field 4: name
    if (thrift_write_string_field(buf, 4, col->name, &last_field) != 0) return -1;

    // Field 5: num_children (only for groups/root)
    if (is_root && num_children > 0) {
        if (thrift_write_i32(buf, 5, num_children, &last_field) != 0) return -1;
    }

    // Field 6: converted_type
    if (!is_root && col->converted_type >= 0) {
        if (thrift_write_i32(buf, 6, col->converted_type, &last_field) != 0) return -1;
    }

    return thrift_write_field_stop(buf);
}

// Serialize ColumnMetaData
static int serialize_column_metadata(ThriftBuffer* buf, ParquetColumnDef* col, ParquetColumnChunkInfo* info, ParquetCompressionCodec codec) {
    int16_t last_field = 0;

    // Field 1: type
    if (thrift_write_i32(buf, 1, col->type, &last_field) != 0) return -1;

    // Field 2: encodings (list<Encoding>)
    if (thrift_write_list_header(buf, 2, THRIFT_CT_I32, info->num_encodings, &last_field) != 0) return -1;
    for (int i = 0; i < info->num_encodings; i++) {
        if (thrift_buffer_write_zigzag(buf, info->encodings[i]) != 0) return -1;
    }

    // Field 3: path_in_schema (list<string>)
    if (thrift_write_list_header(buf, 3, THRIFT_CT_BINARY, 1, &last_field) != 0) return -1;
    if (thrift_buffer_write_string(buf, col->name) != 0) return -1;

    // Field 4: codec
    if (thrift_write_i32(buf, 4, codec, &last_field) != 0) return -1;

    // Field 5: num_values
    if (thrift_write_i64(buf, 5, info->num_values, &last_field) != 0) return -1;

    // Field 6: total_uncompressed_size
    if (thrift_write_i64(buf, 6, info->total_uncompressed_size, &last_field) != 0) return -1;

    // Field 7: total_compressed_size
    if (thrift_write_i64(buf, 7, info->total_compressed_size, &last_field) != 0) return -1;

    // Field 9: data_page_offset
    if (thrift_write_i64(buf, 9, info->data_page_offset, &last_field) != 0) return -1;

    return thrift_write_field_stop(buf);
}

// Serialize ColumnChunk
static int serialize_column_chunk(ThriftBuffer* buf, ParquetColumnDef* col, ParquetColumnChunkInfo* info, ParquetCompressionCodec codec) {
    int16_t last_field = 0;

    // Field 2: file_offset
    if (thrift_write_i64(buf, 2, info->file_offset, &last_field) != 0) return -1;

    // Field 3: meta_data (ColumnMetaData struct)
    if (thrift_write_field_header(buf, 3, THRIFT_CT_STRUCT, &last_field) != 0) return -1;
    if (serialize_column_metadata(buf, col, info, codec) != 0) return -1;

    return thrift_write_field_stop(buf);
}

// Serialize RowGroup
static int serialize_row_group(ThriftBuffer* buf, ParquetRowGroupInfo* rg, ParquetColumnDef* cols, ParquetCompressionCodec codec) {
    int16_t last_field = 0;

    // Field 1: columns (list<ColumnChunk>)
    if (thrift_write_list_header(buf, 1, THRIFT_CT_STRUCT, rg->num_columns, &last_field) != 0) return -1;
    for (int i = 0; i < rg->num_columns; i++) {
        if (serialize_column_chunk(buf, &cols[i], &rg->columns[i], codec) != 0) return -1;
    }

    // Field 2: total_byte_size
    if (thrift_write_i64(buf, 2, rg->total_byte_size, &last_field) != 0) return -1;

    // Field 3: num_rows
    if (thrift_write_i64(buf, 3, rg->num_rows, &last_field) != 0) return -1;

    return thrift_write_field_stop(buf);
}

// Serialize FileMetaData
static int serialize_file_metadata(ThriftBuffer* buf, ParquetFileWriter* writer) {
    int16_t last_field = 0;

    // Field 1: version
    if (thrift_write_i32(buf, 1, PARQUET_VERSION, &last_field) != 0) return -1;

    // Field 2: schema (list<SchemaElement>)
    // First element is root, then columns
    int num_schema_elements = 1 + writer->num_columns;
    if (thrift_write_list_header(buf, 2, THRIFT_CT_STRUCT, num_schema_elements, &last_field) != 0) return -1;

    // Root schema element
    ParquetColumnDef root = { .name = "schema", .type = -1, .repetition = PARQUET_REPETITION_REQUIRED };
    if (serialize_schema_element(buf, &root, true, writer->num_columns) != 0) return -1;

    // Column schema elements
    for (int i = 0; i < writer->num_columns; i++) {
        if (serialize_schema_element(buf, &writer->columns[i], false, 0) != 0) return -1;
    }

    // Field 3: num_rows
    int64_t total_rows = 0;
    for (int i = 0; i < writer->num_row_groups; i++) {
        total_rows += writer->row_groups[i].num_rows;
    }
    if (thrift_write_i64(buf, 3, total_rows, &last_field) != 0) return -1;

    // Field 4: row_groups (list<RowGroup>)
    if (thrift_write_list_header(buf, 4, THRIFT_CT_STRUCT, writer->num_row_groups, &last_field) != 0) return -1;
    for (int i = 0; i < writer->num_row_groups; i++) {
        if (serialize_row_group(buf, &writer->row_groups[i], writer->columns, writer->compression) != 0) return -1;
    }

    // Field 6: created_by
    if (writer->created_by) {
        if (thrift_write_string_field(buf, 6, writer->created_by, &last_field) != 0) return -1;
    }

    return thrift_write_field_stop(buf);
}

// Serialize DataPageHeader
static int serialize_data_page_header(ThriftBuffer* buf, int32_t num_values, int32_t uncompressed_size, int32_t compressed_size, ParquetEncoding encoding) {
    int16_t last_field = 0;

    // Field 1: num_values
    if (thrift_write_i32(buf, 1, num_values, &last_field) != 0) return -1;

    // Field 2: encoding
    if (thrift_write_i32(buf, 2, encoding, &last_field) != 0) return -1;

    // Field 3: definition_level_encoding
    if (thrift_write_i32(buf, 3, PARQUET_ENCODING_RLE, &last_field) != 0) return -1;

    // Field 4: repetition_level_encoding
    if (thrift_write_i32(buf, 4, PARQUET_ENCODING_RLE, &last_field) != 0) return -1;

    return thrift_write_field_stop(buf);
}

// Serialize PageHeader
static int serialize_page_header(ThriftBuffer* buf, ParquetPageType type, int32_t uncompressed_size, int32_t compressed_size, int32_t num_values, ParquetEncoding encoding) {
    int16_t last_field = 0;

    // Field 1: type
    if (thrift_write_i32(buf, 1, type, &last_field) != 0) return -1;

    // Field 2: uncompressed_page_size
    if (thrift_write_i32(buf, 2, uncompressed_size, &last_field) != 0) return -1;

    // Field 3: compressed_page_size
    if (thrift_write_i32(buf, 3, compressed_size, &last_field) != 0) return -1;

    // Field 5: data_page_header (for DATA pages)
    if (type == PARQUET_PAGE_DATA) {
        if (thrift_write_field_header(buf, 5, THRIFT_CT_STRUCT, &last_field) != 0) return -1;
        if (serialize_data_page_header(buf, num_values, uncompressed_size, compressed_size, encoding) != 0) return -1;
    }

    return thrift_write_field_stop(buf);
}

// ============================================================================
// Parquet Writer Implementation
// ============================================================================

ParquetFileWriter* parquet_file_writer_create(const char* path) {
    ParquetFileWriter* writer = calloc(1, sizeof(ParquetFileWriter));
    if (!writer) return NULL;

    writer->file = fopen(path, "wb");
    if (!writer->file) {
        free(writer);
        return NULL;
    }

    writer->file_path = strdup(path);
    writer->compression = PARQUET_CODEC_UNCOMPRESSED;
    writer->row_group_size = 128 * 1024 * 1024;  // 128 MB default
    writer->write_statistics = false;
    writer->created_by = strdup("arrow-lean pure-c-parquet-1.0.0");
    writer->row_groups_capacity = 8;
    writer->row_groups = calloc(writer->row_groups_capacity, sizeof(ParquetRowGroupInfo));

    // Write magic bytes
    fwrite(PARQUET_MAGIC, 1, PARQUET_MAGIC_SIZE, writer->file);
    writer->current_offset = PARQUET_MAGIC_SIZE;

    return writer;
}

void parquet_file_writer_set_compression(ParquetFileWriter* writer, ParquetCompressionCodec codec) {
    if (writer) {
        writer->compression = codec;
    }
}

void parquet_file_writer_set_row_group_size(ParquetFileWriter* writer, int64_t size) {
    if (writer && size > 0) {
        writer->row_group_size = size;
    }
}

int parquet_file_writer_add_column(
    ParquetFileWriter* writer,
    const char* name,
    ParquetType type,
    ParquetConvertedType converted_type,
    ParquetRepetition repetition
) {
    if (!writer || !name) return -1;

    int new_count = writer->num_columns + 1;
    ParquetColumnDef* new_cols = realloc(writer->columns, new_count * sizeof(ParquetColumnDef));
    if (!new_cols) return -1;

    writer->columns = new_cols;
    ParquetColumnDef* col = &writer->columns[writer->num_columns];
    col->name = strdup(name);
    col->type = type;
    col->converted_type = converted_type;
    col->repetition = repetition;
    col->type_length = 0;

    writer->num_columns = new_count;
    return 0;
}

// Convert Arrow format string to Parquet types
static int arrow_format_to_parquet(const char* format, ParquetType* type, ParquetConvertedType* converted) {
    if (!format) return -1;

    *converted = PARQUET_CONVERTED_NONE;

    switch (format[0]) {
        case 'b':  // bool
            *type = PARQUET_TYPE_BOOLEAN;
            break;
        case 'c':  // int8
        case 'C':  // uint8
        case 's':  // int16
        case 'S':  // uint16
        case 'i':  // int32
        case 'I':  // uint32
            *type = PARQUET_TYPE_INT32;
            break;
        case 'l':  // int64
        case 'L':  // uint64
            *type = PARQUET_TYPE_INT64;
            break;
        case 'f':  // float32
            *type = PARQUET_TYPE_FLOAT;
            break;
        case 'g':  // float64
            *type = PARQUET_TYPE_DOUBLE;
            break;
        case 'u':  // utf8 string
        case 'U':  // large utf8 string
            *type = PARQUET_TYPE_BYTE_ARRAY;
            *converted = PARQUET_CONVERTED_UTF8;
            break;
        case 'z':  // binary
        case 'Z':  // large binary
            *type = PARQUET_TYPE_BYTE_ARRAY;
            break;
        case 't':  // timestamp
            *type = PARQUET_TYPE_INT64;
            if (strncmp(format, "tsu", 3) == 0) {
                *converted = PARQUET_CONVERTED_TIMESTAMP_MICROS;
            } else if (strncmp(format, "tsm", 3) == 0) {
                *converted = PARQUET_CONVERTED_TIMESTAMP_MILLIS;
            }
            break;
        case '+':  // nested types (struct, list, etc.)
            // For now, we don't support nested types
            return -1;
        default:
            return -1;
    }

    return 0;
}

int parquet_file_writer_set_schema_from_arrow(ParquetFileWriter* writer, struct ArrowSchema* schema) {
    if (!writer || !schema) return -1;

    // Schema must be a struct (format "+s")
    if (!schema->format || schema->format[0] != '+') return -1;

    for (int64_t i = 0; i < schema->n_children; i++) {
        struct ArrowSchema* child = schema->children[i];
        if (!child || !child->format || !child->name) continue;

        ParquetType type;
        ParquetConvertedType converted;
        if (arrow_format_to_parquet(child->format, &type, &converted) != 0) {
            return -1;
        }

        ParquetRepetition rep = (child->flags & ARROW_FLAG_NULLABLE)
            ? PARQUET_REPETITION_OPTIONAL
            : PARQUET_REPETITION_REQUIRED;

        if (parquet_file_writer_add_column(writer, child->name, type, converted, rep) != 0) {
            return -1;
        }
    }

    return 0;
}

// Write RLE-encoded definition levels
static int write_definition_levels(ThriftBuffer* buf, int num_values, const uint8_t* validity, int64_t null_count) {
    // Definition level: 0 = null, 1 = not null
    // For OPTIONAL columns, we need definition levels

    // RLE encoding: bit-width, then RLE/bit-packed data
    // Bit width for max definition level 1 is 1

    if (null_count == 0) {
        // All values are defined (level 1)
        // Write as RLE run
        int bit_width = 1;
        if (thrift_buffer_write_byte(buf, bit_width) != 0) return -1;

        // RLE header: (count << 1) | 0
        int rle_header = (num_values << 1);
        if (thrift_buffer_write_varint(buf, rle_header) != 0) return -1;

        // Value: 1 (defined)
        if (thrift_buffer_write_byte(buf, 1) != 0) return -1;
    } else {
        // Mix of null and non-null
        // Write as bit-packed
        int bit_width = 1;
        if (thrift_buffer_write_byte(buf, bit_width) != 0) return -1;

        // Bit-packed header: ((count / 8) << 1) | 1
        int num_groups = (num_values + 7) / 8;
        int bp_header = (num_groups << 1) | 1;
        if (thrift_buffer_write_varint(buf, bp_header) != 0) return -1;

        // Copy validity bitmap directly (it's already bit-packed)
        int bitmap_bytes = (num_values + 7) / 8;
        if (thrift_buffer_write_bytes(buf, validity, bitmap_bytes) != 0) return -1;
    }

    return 0;
}

// Write plain-encoded data for different types
static int write_plain_int64_data(ThriftBuffer* buf, const int64_t* data, int num_values, const uint8_t* validity) {
    for (int i = 0; i < num_values; i++) {
        // Skip nulls
        if (validity) {
            int byte_idx = i / 8;
            int bit_idx = i % 8;
            if (!((validity[byte_idx] >> bit_idx) & 1)) continue;
        }
        if (thrift_buffer_write_bytes(buf, &data[i], sizeof(int64_t)) != 0) return -1;
    }
    return 0;
}

static int write_plain_double_data(ThriftBuffer* buf, const double* data, int num_values, const uint8_t* validity) {
    for (int i = 0; i < num_values; i++) {
        if (validity) {
            int byte_idx = i / 8;
            int bit_idx = i % 8;
            if (!((validity[byte_idx] >> bit_idx) & 1)) continue;
        }
        if (thrift_buffer_write_bytes(buf, &data[i], sizeof(double)) != 0) return -1;
    }
    return 0;
}

static int write_plain_bool_data(ThriftBuffer* buf, const uint8_t* data, int num_values, const uint8_t* validity) {
    for (int i = 0; i < num_values; i++) {
        if (validity) {
            int byte_idx = i / 8;
            int bit_idx = i % 8;
            if (!((validity[byte_idx] >> bit_idx) & 1)) continue;
        }
        int data_byte_idx = i / 8;
        int data_bit_idx = i % 8;
        uint8_t value = (data[data_byte_idx] >> data_bit_idx) & 1;
        if (thrift_buffer_write_byte(buf, value) != 0) return -1;
    }
    return 0;
}

static int write_plain_string_data(ThriftBuffer* buf, const int32_t* offsets, const char* data, int num_values, const uint8_t* validity) {
    for (int i = 0; i < num_values; i++) {
        if (validity) {
            int byte_idx = i / 8;
            int bit_idx = i % 8;
            if (!((validity[byte_idx] >> bit_idx) & 1)) continue;
        }
        int32_t start = offsets[i];
        int32_t end = offsets[i + 1];
        int32_t len = end - start;

        // Write length as 4-byte little-endian
        if (thrift_buffer_write_bytes(buf, &len, sizeof(int32_t)) != 0) return -1;
        // Write string data
        if (len > 0) {
            if (thrift_buffer_write_bytes(buf, data + start, len) != 0) return -1;
        }
    }
    return 0;
}

// Write a column chunk
static int write_column_chunk(ParquetFileWriter* writer, struct ArrowArray* array, ParquetColumnDef* col, ParquetColumnChunkInfo* info) {
    // Build the data page
    ThriftBuffer* data_buf = thrift_buffer_create(4096);
    if (!data_buf) return -1;

    int num_values = (int)array->length;
    const uint8_t* validity = (const uint8_t*)array->buffers[0];
    int64_t null_count = array->null_count;

    // Write definition levels if column is optional
    if (col->repetition == PARQUET_REPETITION_OPTIONAL) {
        if (write_definition_levels(data_buf, num_values, validity, null_count) != 0) {
            thrift_buffer_free(data_buf);
            return -1;
        }
    }

    // Write data based on type
    int result = 0;
    switch (col->type) {
        case PARQUET_TYPE_INT64:
            result = write_plain_int64_data(data_buf, (const int64_t*)array->buffers[1], num_values, validity);
            break;
        case PARQUET_TYPE_DOUBLE:
            result = write_plain_double_data(data_buf, (const double*)array->buffers[1], num_values, validity);
            break;
        case PARQUET_TYPE_BOOLEAN:
            result = write_plain_bool_data(data_buf, (const uint8_t*)array->buffers[1], num_values, validity);
            break;
        case PARQUET_TYPE_BYTE_ARRAY:
            result = write_plain_string_data(data_buf,
                (const int32_t*)array->buffers[1],
                (const char*)array->buffers[2],
                num_values, validity);
            break;
        default:
            result = -1;
    }

    if (result != 0) {
        thrift_buffer_free(data_buf);
        return -1;
    }

    // Compress data page if compression is enabled
    int32_t uncompressed_size = (int32_t)data_buf->size;
    uint8_t* write_data = data_buf->data;
    int32_t compressed_size = uncompressed_size;
    uint8_t* compressed_buf = NULL;

    if (writer->compression == PARQUET_CODEC_ZSTD && uncompressed_size > 0) {
        size_t bound = ZSTD_compressBound(uncompressed_size);
        compressed_buf = malloc(bound);
        if (compressed_buf) {
            size_t csize = ZSTD_compress(compressed_buf, bound,
                                         data_buf->data, uncompressed_size, 3);
            if (!ZSTD_isError(csize)) {
                compressed_size = (int32_t)csize;
                write_data = compressed_buf;
            } else {
                free(compressed_buf);
                compressed_buf = NULL;
                // Fall back to uncompressed
            }
        }
    }

    // Build page header
    ThriftBuffer* header_buf = thrift_buffer_create(256);
    if (!header_buf) {
        free(compressed_buf);
        thrift_buffer_free(data_buf);
        return -1;
    }

    if (serialize_page_header(header_buf, PARQUET_PAGE_DATA, uncompressed_size, compressed_size, num_values, PARQUET_ENCODING_PLAIN) != 0) {
        free(compressed_buf);
        thrift_buffer_free(data_buf);
        thrift_buffer_free(header_buf);
        return -1;
    }

    // Record column chunk info
    info->file_offset = writer->current_offset;
    info->data_page_offset = writer->current_offset;
    info->total_uncompressed_size = header_buf->size + uncompressed_size;
    info->total_compressed_size = header_buf->size + compressed_size;
    info->num_values = num_values;

    // Set encodings
    info->num_encodings = col->repetition == PARQUET_REPETITION_OPTIONAL ? 2 : 1;
    info->encodings = malloc(info->num_encodings * sizeof(ParquetEncoding));
    info->encodings[0] = PARQUET_ENCODING_PLAIN;
    if (info->num_encodings > 1) {
        info->encodings[1] = PARQUET_ENCODING_RLE;  // For definition levels
    }

    // Write to file
    fwrite(header_buf->data, 1, header_buf->size, writer->file);
    fwrite(write_data, 1, compressed_size, writer->file);
    writer->current_offset += header_buf->size + compressed_size;

    free(compressed_buf);
    thrift_buffer_free(data_buf);
    thrift_buffer_free(header_buf);

    return 0;
}

int parquet_file_writer_write_batch(ParquetFileWriter* writer, struct ArrowArray* array, struct ArrowSchema* schema) {
    if (!writer || !array || !schema) return -1;

    // Set schema from Arrow if not already set
    if (writer->num_columns == 0) {
        if (parquet_file_writer_set_schema_from_arrow(writer, schema) != 0) {
            return -1;
        }
    }

    // The array should be a struct array with children
    if (array->n_children != writer->num_columns) {
        return -1;
    }

    // Create row group info
    if (writer->num_row_groups >= writer->row_groups_capacity) {
        int new_cap = writer->row_groups_capacity * 2;
        ParquetRowGroupInfo* new_rgs = realloc(writer->row_groups, new_cap * sizeof(ParquetRowGroupInfo));
        if (!new_rgs) return -1;
        writer->row_groups = new_rgs;
        writer->row_groups_capacity = new_cap;
    }

    ParquetRowGroupInfo* rg = &writer->row_groups[writer->num_row_groups];
    memset(rg, 0, sizeof(ParquetRowGroupInfo));
    rg->num_rows = array->length;
    rg->num_columns = writer->num_columns;
    rg->columns = calloc(writer->num_columns, sizeof(ParquetColumnChunkInfo));

    // Write each column
    int64_t total_size = 0;
    for (int i = 0; i < writer->num_columns; i++) {
        struct ArrowArray* col_array = array->children[i];
        if (write_column_chunk(writer, col_array, &writer->columns[i], &rg->columns[i]) != 0) {
            // Cleanup on error
            for (int j = 0; j < i; j++) {
                free(rg->columns[j].encodings);
            }
            free(rg->columns);
            return -1;
        }
        total_size += rg->columns[i].total_compressed_size;
    }

    rg->total_byte_size = total_size;
    writer->num_row_groups++;

    return 0;
}

int parquet_file_writer_close(ParquetFileWriter* writer) {
    if (!writer || !writer->file) return -1;

    // Serialize footer (FileMetaData)
    ThriftBuffer* footer = thrift_buffer_create(4096);
    if (!footer) return -1;

    if (serialize_file_metadata(footer, writer) != 0) {
        thrift_buffer_free(footer);
        return -1;
    }

    // Write footer
    fwrite(footer->data, 1, footer->size, writer->file);

    // Write footer size (4 bytes, little-endian)
    uint32_t footer_size = (uint32_t)footer->size;
    fwrite(&footer_size, sizeof(uint32_t), 1, writer->file);

    // Write magic bytes
    fwrite(PARQUET_MAGIC, 1, PARQUET_MAGIC_SIZE, writer->file);

    thrift_buffer_free(footer);
    fclose(writer->file);
    writer->file = NULL;

    return 0;
}

void parquet_file_writer_free(ParquetFileWriter* writer) {
    if (!writer) return;

    if (writer->file) {
        fclose(writer->file);
    }

    free(writer->file_path);
    free(writer->created_by);

    for (int i = 0; i < writer->num_columns; i++) {
        free(writer->columns[i].name);
    }
    free(writer->columns);

    for (int i = 0; i < writer->num_row_groups; i++) {
        for (int j = 0; j < writer->row_groups[i].num_columns; j++) {
            free(writer->row_groups[i].columns[j].encodings);
        }
        free(writer->row_groups[i].columns);
    }
    free(writer->row_groups);

    free(writer);
}

// ============================================================================
// High-level API
// ============================================================================

int write_arrow_to_parquet(
    const char* path,
    struct ArrowSchema* schema,
    struct ArrowArray* array,
    ParquetCompressionCodec compression
) {
    ParquetFileWriter* writer = parquet_file_writer_create(path);
    if (!writer) return -1;

    parquet_file_writer_set_compression(writer, compression);

    if (parquet_file_writer_write_batch(writer, array, schema) != 0) {
        parquet_file_writer_free(writer);
        return -1;
    }

    int result = parquet_file_writer_close(writer);
    parquet_file_writer_free(writer);

    return result;
}

int write_arrow_stream_to_parquet(
    const char* path,
    struct ArrowArrayStream* stream,
    ParquetCompressionCodec compression
) {
    if (!stream || !path) return -1;

    // Get schema
    struct ArrowSchema schema;
    memset(&schema, 0, sizeof(schema));
    if (stream->get_schema(stream, &schema) != 0) {
        return -1;
    }

    ParquetFileWriter* writer = parquet_file_writer_create(path);
    if (!writer) {
        if (schema.release) schema.release(&schema);
        return -1;
    }

    parquet_file_writer_set_compression(writer, compression);

    // Write all batches from stream
    struct ArrowArray array;
    while (1) {
        memset(&array, 0, sizeof(array));
        if (stream->get_next(stream, &array) != 0) {
            parquet_file_writer_free(writer);
            if (schema.release) schema.release(&schema);
            return -1;
        }

        if (array.release == NULL) {
            // End of stream
            break;
        }

        int result = parquet_file_writer_write_batch(writer, &array, &schema);
        if (array.release) array.release(&array);

        if (result != 0) {
            parquet_file_writer_free(writer);
            if (schema.release) schema.release(&schema);
            return -1;
        }
    }

    int result = parquet_file_writer_close(writer);
    parquet_file_writer_free(writer);
    if (schema.release) schema.release(&schema);

    return result;
}
