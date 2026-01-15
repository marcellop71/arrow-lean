// Enable POSIX extensions
#define _POSIX_C_SOURCE 200809L

#include "parquet_reader_impl.h"
#include "arrow_builders.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// ============================================================================
// Thrift Compact Protocol Reader Implementation
// ============================================================================

// Compact protocol type IDs (same as writer)
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

void thrift_reader_init(ThriftReader* reader, const uint8_t* data, size_t size) {
    reader->data = data;
    reader->size = size;
    reader->pos = 0;
}

int thrift_reader_read_byte(ThriftReader* reader, uint8_t* out) {
    if (reader->pos >= reader->size) return -1;
    *out = reader->data[reader->pos++];
    return 0;
}

int thrift_reader_read_bytes(ThriftReader* reader, void* out, size_t len) {
    if (reader->pos + len > reader->size) return -1;
    memcpy(out, reader->data + reader->pos, len);
    reader->pos += len;
    return 0;
}

int thrift_reader_read_varint(ThriftReader* reader, uint64_t* out) {
    uint64_t result = 0;
    int shift = 0;
    uint8_t byte;

    do {
        if (thrift_reader_read_byte(reader, &byte) != 0) return -1;
        result |= (uint64_t)(byte & 0x7F) << shift;
        shift += 7;
        if (shift > 70) return -1;  // Overflow protection
    } while (byte & 0x80);

    *out = result;
    return 0;
}

int thrift_reader_read_zigzag(ThriftReader* reader, int64_t* out) {
    uint64_t varint;
    if (thrift_reader_read_varint(reader, &varint) != 0) return -1;
    // Zigzag decode: (n >> 1) ^ -(n & 1)
    *out = (int64_t)((varint >> 1) ^ (-(int64_t)(varint & 1)));
    return 0;
}

int thrift_reader_read_string(ThriftReader* reader, char** out) {
    uint64_t len;
    if (thrift_reader_read_varint(reader, &len) != 0) return -1;

    if (len == 0) {
        *out = malloc(1);
        if (!*out) return -1;
        (*out)[0] = '\0';
        return 0;
    }

    if (reader->pos + len > reader->size) return -1;

    *out = malloc(len + 1);
    if (!*out) return -1;

    memcpy(*out, reader->data + reader->pos, len);
    (*out)[len] = '\0';
    reader->pos += len;
    return 0;
}

int thrift_reader_read_binary(ThriftReader* reader, uint8_t** out, size_t* out_len) {
    uint64_t len;
    if (thrift_reader_read_varint(reader, &len) != 0) return -1;

    if (len == 0) {
        *out = NULL;
        *out_len = 0;
        return 0;
    }

    if (reader->pos + len > reader->size) return -1;

    *out = malloc(len);
    if (!*out) return -1;

    memcpy(*out, reader->data + reader->pos, len);
    *out_len = len;
    reader->pos += len;
    return 0;
}

int thrift_reader_read_field_header(ThriftReader* reader, int16_t* field_id,
                                     uint8_t* type, int16_t last_field_id) {
    uint8_t byte;
    if (thrift_reader_read_byte(reader, &byte) != 0) return -1;

    if (byte == THRIFT_CT_STOP) {
        *field_id = 0;
        *type = THRIFT_CT_STOP;
        return 0;
    }

    uint8_t delta = (byte >> 4) & 0x0F;
    *type = byte & 0x0F;

    if (delta == 0) {
        // Long form: read field id as zigzag
        int64_t id;
        if (thrift_reader_read_zigzag(reader, &id) != 0) return -1;
        *field_id = (int16_t)id;
    } else {
        // Short form: delta from last field
        *field_id = last_field_id + delta;
    }

    return 0;
}

int thrift_reader_read_list_header(ThriftReader* reader, uint8_t* elem_type, size_t* count) {
    uint8_t byte;
    if (thrift_reader_read_byte(reader, &byte) != 0) return -1;

    uint8_t size_and_type = byte;
    *elem_type = size_and_type & 0x0F;
    size_t size = (size_and_type >> 4) & 0x0F;

    if (size == 15) {
        // Long form: read size as varint
        uint64_t len;
        if (thrift_reader_read_varint(reader, &len) != 0) return -1;
        *count = (size_t)len;
    } else {
        *count = size;
    }

    return 0;
}

int thrift_reader_skip_field(ThriftReader* reader, uint8_t type) {
    switch (type) {
        case THRIFT_CT_BOOLEAN_TRUE:
        case THRIFT_CT_BOOLEAN_FALSE:
            return 0;  // No data to skip

        case THRIFT_CT_BYTE: {
            uint8_t dummy;
            return thrift_reader_read_byte(reader, &dummy);
        }

        case THRIFT_CT_I16:
        case THRIFT_CT_I32:
        case THRIFT_CT_I64: {
            int64_t dummy;
            return thrift_reader_read_zigzag(reader, &dummy);
        }

        case THRIFT_CT_DOUBLE: {
            if (reader->pos + 8 > reader->size) return -1;
            reader->pos += 8;
            return 0;
        }

        case THRIFT_CT_BINARY: {
            uint64_t len;
            if (thrift_reader_read_varint(reader, &len) != 0) return -1;
            if (reader->pos + len > reader->size) return -1;
            reader->pos += len;
            return 0;
        }

        case THRIFT_CT_LIST:
        case THRIFT_CT_SET: {
            uint8_t elem_type;
            size_t count;
            if (thrift_reader_read_list_header(reader, &elem_type, &count) != 0) return -1;
            for (size_t i = 0; i < count; i++) {
                if (thrift_reader_skip_field(reader, elem_type) != 0) return -1;
            }
            return 0;
        }

        case THRIFT_CT_MAP: {
            uint8_t byte;
            if (thrift_reader_read_byte(reader, &byte) != 0) return -1;
            if (byte == 0) return 0;  // Empty map

            uint64_t count;
            if (thrift_reader_read_varint(reader, &count) != 0) return -1;
            uint8_t key_type = (byte >> 4) & 0x0F;
            uint8_t val_type = byte & 0x0F;
            for (uint64_t i = 0; i < count; i++) {
                if (thrift_reader_skip_field(reader, key_type) != 0) return -1;
                if (thrift_reader_skip_field(reader, val_type) != 0) return -1;
            }
            return 0;
        }

        case THRIFT_CT_STRUCT: {
            int16_t field_id;
            uint8_t field_type;
            int16_t last_field = 0;
            while (1) {
                if (thrift_reader_read_field_header(reader, &field_id, &field_type, last_field) != 0) return -1;
                if (field_type == THRIFT_CT_STOP) break;
                if (thrift_reader_skip_field(reader, field_type) != 0) return -1;
                last_field = field_id;
            }
            return 0;
        }

        default:
            return -1;
    }
}

// ============================================================================
// RLE/Bit-Packing Decoder Implementation
// ============================================================================

void rle_decoder_init(RleDecoder* decoder, const uint8_t* data, size_t size, int bit_width) {
    decoder->data = data;
    decoder->size = size;
    decoder->pos = 0;
    decoder->bit_width = bit_width;
    decoder->current_value = 0;
    decoder->remaining_in_run = 0;
    decoder->is_literal_run = false;
    decoder->bit_buffer = 0;
    decoder->bits_in_buffer = 0;
}

static int rle_read_unsigned_vlq(RleDecoder* decoder, uint32_t* out) {
    uint32_t result = 0;
    int shift = 0;
    uint8_t byte;

    do {
        if (decoder->pos >= decoder->size) return -1;
        byte = decoder->data[decoder->pos++];
        result |= (uint32_t)(byte & 0x7F) << shift;
        shift += 7;
        if (shift > 28) return -1;  // Overflow protection
    } while (byte & 0x80);

    *out = result;
    return 0;
}

static int rle_read_bits(RleDecoder* decoder, int num_bits, int32_t* out) {
    if (num_bits == 0) {
        *out = 0;
        return 0;
    }

    // Refill bit buffer
    while (decoder->bits_in_buffer < num_bits && decoder->pos < decoder->size) {
        decoder->bit_buffer |= (uint64_t)decoder->data[decoder->pos++] << decoder->bits_in_buffer;
        decoder->bits_in_buffer += 8;
    }

    if (decoder->bits_in_buffer < num_bits) return -1;

    *out = (int32_t)(decoder->bit_buffer & ((1ULL << num_bits) - 1));
    decoder->bit_buffer >>= num_bits;
    decoder->bits_in_buffer -= num_bits;
    return 0;
}

int rle_decoder_next(RleDecoder* decoder, int32_t* out) {
    if (decoder->remaining_in_run > 0) {
        if (decoder->is_literal_run) {
            // Read next value from bit-packed literals
            if (rle_read_bits(decoder, decoder->bit_width, out) != 0) return -1;
        } else {
            // Return repeated value
            *out = decoder->current_value;
        }
        decoder->remaining_in_run--;
        return 0;
    }

    // Need to read next run header
    if (decoder->pos >= decoder->size) return -1;

    uint32_t header;
    if (rle_read_unsigned_vlq(decoder, &header) != 0) return -1;

    decoder->is_literal_run = (header & 1) != 0;

    if (decoder->is_literal_run) {
        // Literal run: count is (header >> 1) * 8
        decoder->remaining_in_run = (header >> 1) * 8;
        if (decoder->remaining_in_run == 0) return -1;

        // Reset bit buffer for reading literals
        decoder->bit_buffer = 0;
        decoder->bits_in_buffer = 0;

        // Read first value
        if (rle_read_bits(decoder, decoder->bit_width, out) != 0) return -1;
        decoder->remaining_in_run--;
    } else {
        // RLE run: count is (header >> 1)
        decoder->remaining_in_run = header >> 1;
        if (decoder->remaining_in_run == 0) return -1;

        // Read the repeated value (byte-aligned)
        int num_bytes = (decoder->bit_width + 7) / 8;
        int32_t value = 0;
        for (int i = 0; i < num_bytes; i++) {
            if (decoder->pos >= decoder->size) return -1;
            value |= (int32_t)decoder->data[decoder->pos++] << (i * 8);
        }
        decoder->current_value = value;
        *out = value;
        decoder->remaining_in_run--;
    }

    return 0;
}

int rle_decoder_decode_batch(RleDecoder* decoder, int32_t* out, int count) {
    for (int i = 0; i < count; i++) {
        if (rle_decoder_next(decoder, &out[i]) != 0) return i;
    }
    return count;
}

// ============================================================================
// Plain Encoding Decoders
// ============================================================================

int decode_plain_boolean(const uint8_t* data, size_t size, bool* out, int count) {
    int decoded = 0;
    for (int i = 0; i < count && i / 8 < (int)size; i++) {
        int byte_idx = i / 8;
        int bit_idx = i % 8;
        out[i] = (data[byte_idx] >> bit_idx) & 1;
        decoded++;
    }
    return decoded;
}

int decode_plain_int32(const uint8_t* data, size_t size, int32_t* out, int count) {
    int max_count = (int)(size / 4);
    if (count > max_count) count = max_count;
    memcpy(out, data, count * 4);
    return count;
}

int decode_plain_int64(const uint8_t* data, size_t size, int64_t* out, int count) {
    int max_count = (int)(size / 8);
    if (count > max_count) count = max_count;
    memcpy(out, data, count * 8);
    return count;
}

int decode_plain_float(const uint8_t* data, size_t size, float* out, int count) {
    int max_count = (int)(size / 4);
    if (count > max_count) count = max_count;
    memcpy(out, data, count * 4);
    return count;
}

int decode_plain_double(const uint8_t* data, size_t size, double* out, int count) {
    int max_count = (int)(size / 8);
    if (count > max_count) count = max_count;
    memcpy(out, data, count * 8);
    return count;
}

int decode_plain_byte_array(const uint8_t* data, size_t size,
                            int32_t** offsets, uint8_t** values,
                            int count, size_t* values_len) {
    // First pass: calculate total size
    size_t pos = 0;
    size_t total_len = 0;
    int actual_count = 0;

    while (pos + 4 <= size && actual_count < count) {
        uint32_t len;
        memcpy(&len, data + pos, 4);
        pos += 4;
        if (pos + len > size) break;
        total_len += len;
        pos += len;
        actual_count++;
    }

    if (actual_count == 0) {
        *offsets = NULL;
        *values = NULL;
        *values_len = 0;
        return 0;
    }

    // Allocate buffers
    *offsets = malloc((actual_count + 1) * sizeof(int32_t));
    *values = malloc(total_len);
    if (!*offsets || !*values) {
        free(*offsets);
        free(*values);
        return -1;
    }

    // Second pass: copy data
    pos = 0;
    size_t val_pos = 0;
    (*offsets)[0] = 0;

    for (int i = 0; i < actual_count; i++) {
        uint32_t len;
        memcpy(&len, data + pos, 4);
        pos += 4;
        memcpy(*values + val_pos, data + pos, len);
        pos += len;
        val_pos += len;
        (*offsets)[i + 1] = (int32_t)val_pos;
    }

    *values_len = total_len;
    return actual_count;
}

// ============================================================================
// Metadata Parsing
// ============================================================================

void parquet_page_header_init(ParquetPageHeader* header) {
    memset(header, 0, sizeof(*header));
    header->encoding = PARQUET_ENCODING_PLAIN;
    header->definition_level_encoding = PARQUET_ENCODING_RLE;
    header->repetition_level_encoding = PARQUET_ENCODING_RLE;
}

int parquet_parse_page_header(ThriftReader* reader, ParquetPageHeader* out) {
    parquet_page_header_init(out);

    int16_t field_id;
    uint8_t type;
    int16_t last_field = 0;

    while (1) {
        if (thrift_reader_read_field_header(reader, &field_id, &type, last_field) != 0) return -1;
        if (type == THRIFT_CT_STOP) break;

        switch (field_id) {
            case 1: {  // type
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                out->type = (ParquetPageType)val;
                break;
            }
            case 2: {  // uncompressed_page_size
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                out->uncompressed_page_size = (int32_t)val;
                break;
            }
            case 3: {  // compressed_page_size
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                out->compressed_page_size = (int32_t)val;
                break;
            }
            case 4: {  // crc
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                out->crc = (int32_t)val;
                break;
            }
            case 5: {  // data_page_header (struct)
                int16_t sub_field_id;
                uint8_t sub_type;
                int16_t sub_last_field = 0;
                while (1) {
                    if (thrift_reader_read_field_header(reader, &sub_field_id, &sub_type, sub_last_field) != 0) return -1;
                    if (sub_type == THRIFT_CT_STOP) break;
                    switch (sub_field_id) {
                        case 1: {  // num_values
                            int64_t val;
                            if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                            out->num_values = (int32_t)val;
                            break;
                        }
                        case 2: {  // encoding
                            int64_t val;
                            if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                            out->encoding = (ParquetEncoding)val;
                            break;
                        }
                        case 3: {  // definition_level_encoding
                            int64_t val;
                            if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                            out->definition_level_encoding = (ParquetEncoding)val;
                            break;
                        }
                        case 4: {  // repetition_level_encoding
                            int64_t val;
                            if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                            out->repetition_level_encoding = (ParquetEncoding)val;
                            break;
                        }
                        default:
                            if (thrift_reader_skip_field(reader, sub_type) != 0) return -1;
                    }
                    sub_last_field = sub_field_id;
                }
                break;
            }
            case 7: {  // dictionary_page_header (struct)
                int16_t sub_field_id;
                uint8_t sub_type;
                int16_t sub_last_field = 0;
                while (1) {
                    if (thrift_reader_read_field_header(reader, &sub_field_id, &sub_type, sub_last_field) != 0) return -1;
                    if (sub_type == THRIFT_CT_STOP) break;
                    switch (sub_field_id) {
                        case 1: {  // num_values
                            int64_t val;
                            if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                            out->num_dict_values = (int32_t)val;
                            break;
                        }
                        case 2: {  // encoding
                            int64_t val;
                            if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                            out->dict_encoding = (ParquetEncoding)val;
                            break;
                        }
                        default:
                            if (thrift_reader_skip_field(reader, sub_type) != 0) return -1;
                    }
                    sub_last_field = sub_field_id;
                }
                break;
            }
            case 8: {  // data_page_header_v2 (struct)
                int16_t sub_field_id;
                uint8_t sub_type;
                int16_t sub_last_field = 0;
                while (1) {
                    if (thrift_reader_read_field_header(reader, &sub_field_id, &sub_type, sub_last_field) != 0) return -1;
                    if (sub_type == THRIFT_CT_STOP) break;
                    switch (sub_field_id) {
                        case 1: {  // num_values
                            int64_t val;
                            if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                            out->num_values = (int32_t)val;
                            break;
                        }
                        case 2: {  // num_nulls
                            int64_t val;
                            if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                            out->num_nulls = (int32_t)val;
                            break;
                        }
                        case 3: {  // num_rows
                            int64_t val;
                            if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                            out->num_rows = (int32_t)val;
                            break;
                        }
                        case 4: {  // encoding
                            int64_t val;
                            if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                            out->encoding = (ParquetEncoding)val;
                            break;
                        }
                        case 7: {  // is_compressed
                            out->is_compressed = (sub_type == THRIFT_CT_BOOLEAN_TRUE);
                            break;
                        }
                        default:
                            if (thrift_reader_skip_field(reader, sub_type) != 0) return -1;
                    }
                    sub_last_field = sub_field_id;
                }
                break;
            }
            default:
                if (thrift_reader_skip_field(reader, type) != 0) return -1;
        }
        last_field = field_id;
    }

    return 0;
}

// ============================================================================
// Schema Element Parsing
// ============================================================================

static int parse_schema_element(ThriftReader* reader, ParquetSchemaElement* elem) {
    memset(elem, 0, sizeof(*elem));
    elem->type = -1;
    elem->converted_type = PARQUET_CONVERTED_NONE;

    int16_t field_id;
    uint8_t type;
    int16_t last_field = 0;

    while (1) {
        if (thrift_reader_read_field_header(reader, &field_id, &type, last_field) != 0) return -1;
        if (type == THRIFT_CT_STOP) break;

        switch (field_id) {
            case 1: {  // type
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                elem->type = (ParquetType)val;
                break;
            }
            case 2: {  // type_length
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                elem->type_length = (int32_t)val;
                break;
            }
            case 3: {  // repetition_type
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                elem->repetition = (ParquetRepetition)val;
                break;
            }
            case 4: {  // name
                if (thrift_reader_read_string(reader, &elem->name) != 0) return -1;
                break;
            }
            case 5: {  // num_children
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                elem->num_children = (int32_t)val;
                break;
            }
            case 6: {  // converted_type
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                elem->converted_type = (ParquetConvertedType)val;
                break;
            }
            case 7: {  // scale (for DECIMAL)
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                elem->scale = (int32_t)val;
                break;
            }
            case 8: {  // precision (for DECIMAL)
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                elem->precision = (int32_t)val;
                break;
            }
            default:
                if (thrift_reader_skip_field(reader, type) != 0) return -1;
        }
        last_field = field_id;
    }

    return 0;
}

// ============================================================================
// Column Chunk Metadata Parsing
// ============================================================================

static int parse_column_metadata(ThriftReader* reader, ParquetColumnChunkMeta* meta) {
    memset(meta, 0, sizeof(*meta));

    int16_t field_id;
    uint8_t type;
    int16_t last_field = 0;

    while (1) {
        if (thrift_reader_read_field_header(reader, &field_id, &type, last_field) != 0) return -1;
        if (type == THRIFT_CT_STOP) break;

        switch (field_id) {
            case 1: {  // type
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                meta->type = (ParquetType)val;
                break;
            }
            case 2: {  // encodings (list)
                uint8_t elem_type;
                size_t count;
                if (thrift_reader_read_list_header(reader, &elem_type, &count) != 0) return -1;
                meta->encodings = malloc(count * sizeof(ParquetEncoding));
                meta->num_encodings = (int)count;
                for (size_t i = 0; i < count; i++) {
                    int64_t val;
                    if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                    meta->encodings[i] = (ParquetEncoding)val;
                }
                break;
            }
            case 3: {  // path_in_schema (list of strings)
                uint8_t elem_type;
                size_t count;
                if (thrift_reader_read_list_header(reader, &elem_type, &count) != 0) return -1;
                // Just read the last element as the column name
                char* last_name = NULL;
                for (size_t i = 0; i < count; i++) {
                    free(last_name);
                    if (thrift_reader_read_string(reader, &last_name) != 0) return -1;
                }
                meta->path_in_schema = last_name;
                break;
            }
            case 4: {  // codec
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                meta->codec = (ParquetCompressionCodec)val;
                break;
            }
            case 5: {  // num_values
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                meta->num_values = val;
                break;
            }
            case 6: {  // total_uncompressed_size
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                meta->total_uncompressed_size = val;
                break;
            }
            case 7: {  // total_compressed_size
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                meta->total_compressed_size = val;
                break;
            }
            case 9: {  // data_page_offset
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                meta->data_page_offset = val;
                break;
            }
            case 11: {  // dictionary_page_offset
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                meta->dictionary_page_offset = val;
                break;
            }
            default:
                if (thrift_reader_skip_field(reader, type) != 0) return -1;
        }
        last_field = field_id;
    }

    return 0;
}

// ============================================================================
// Column Chunk Parsing (wraps column metadata)
// ============================================================================

static int parse_column_chunk(ThriftReader* reader, ParquetColumnChunkMeta* chunk) {
    memset(chunk, 0, sizeof(*chunk));

    int16_t field_id;
    uint8_t type;
    int16_t last_field = 0;

    while (1) {
        if (thrift_reader_read_field_header(reader, &field_id, &type, last_field) != 0) return -1;
        if (type == THRIFT_CT_STOP) break;

        switch (field_id) {
            case 1: {  // file_path (optional, for external files)
                char* path;
                if (thrift_reader_read_string(reader, &path) != 0) return -1;
                free(path);  // We don't use external files
                break;
            }
            case 2: {  // file_offset
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                chunk->file_offset = val;
                break;
            }
            case 3: {  // meta_data (ColumnMetaData struct)
                if (parse_column_metadata(reader, chunk) != 0) return -1;
                break;
            }
            default:
                if (thrift_reader_skip_field(reader, type) != 0) return -1;
        }
        last_field = field_id;
    }

    return 0;
}

// ============================================================================
// Row Group Parsing
// ============================================================================

static int parse_row_group(ThriftReader* reader, ParquetRowGroupMeta* rg) {
    memset(rg, 0, sizeof(*rg));

    int16_t field_id;
    uint8_t type;
    int16_t last_field = 0;

    while (1) {
        if (thrift_reader_read_field_header(reader, &field_id, &type, last_field) != 0) return -1;
        if (type == THRIFT_CT_STOP) break;

        switch (field_id) {
            case 1: {  // columns (list of ColumnChunk)
                uint8_t elem_type;
                size_t count;
                if (thrift_reader_read_list_header(reader, &elem_type, &count) != 0) return -1;
                rg->columns = calloc(count, sizeof(ParquetColumnChunkMeta));
                rg->num_columns = (int)count;
                for (size_t i = 0; i < count; i++) {
                    if (parse_column_chunk(reader, &rg->columns[i]) != 0) return -1;
                }
                break;
            }
            case 2: {  // total_byte_size
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                rg->total_byte_size = val;
                break;
            }
            case 3: {  // num_rows
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                rg->num_rows = val;
                break;
            }
            case 6: {  // file_offset
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                rg->file_offset = val;
                break;
            }
            default:
                if (thrift_reader_skip_field(reader, type) != 0) return -1;
        }
        last_field = field_id;
    }

    return 0;
}

// ============================================================================
// FileMetaData Parsing
// ============================================================================

int parquet_parse_file_metadata(ThriftReader* reader, ParquetFileMeta* meta) {
    memset(meta, 0, sizeof(*meta));

    int16_t field_id;
    uint8_t type;
    int16_t last_field = 0;

    while (1) {
        if (thrift_reader_read_field_header(reader, &field_id, &type, last_field) != 0) return -1;
        if (type == THRIFT_CT_STOP) break;

        switch (field_id) {
            case 1: {  // version
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                meta->version = (int32_t)val;
                break;
            }
            case 2: {  // schema (list of SchemaElement)
                uint8_t elem_type;
                size_t count;
                if (thrift_reader_read_list_header(reader, &elem_type, &count) != 0) return -1;
                meta->schema = calloc(count, sizeof(ParquetSchemaElement));
                meta->num_schema_elements = (int)count;
                for (size_t i = 0; i < count; i++) {
                    if (parse_schema_element(reader, &meta->schema[i]) != 0) return -1;
                }
                break;
            }
            case 3: {  // num_rows
                int64_t val;
                if (thrift_reader_read_zigzag(reader, &val) != 0) return -1;
                meta->num_rows = val;
                break;
            }
            case 4: {  // row_groups (list of RowGroup)
                uint8_t elem_type;
                size_t count;
                if (thrift_reader_read_list_header(reader, &elem_type, &count) != 0) return -1;
                meta->row_groups = calloc(count, sizeof(ParquetRowGroupMeta));
                meta->num_row_groups = (int)count;
                for (size_t i = 0; i < count; i++) {
                    if (parse_row_group(reader, &meta->row_groups[i]) != 0) return -1;
                }
                break;
            }
            case 5: {  // key_value_metadata (list of KeyValue)
                uint8_t elem_type;
                size_t count;
                if (thrift_reader_read_list_header(reader, &elem_type, &count) != 0) return -1;
                meta->kv_keys = calloc(count, sizeof(char*));
                meta->kv_values = calloc(count, sizeof(char*));
                meta->num_kv = (int)count;
                for (size_t i = 0; i < count; i++) {
                    // Parse KeyValue struct
                    int16_t kv_field;
                    uint8_t kv_type;
                    int16_t kv_last_field = 0;
                    while (1) {
                        if (thrift_reader_read_field_header(reader, &kv_field, &kv_type, kv_last_field) != 0) return -1;
                        if (kv_type == THRIFT_CT_STOP) break;
                        if (kv_field == 1) {
                            if (thrift_reader_read_string(reader, &meta->kv_keys[i]) != 0) return -1;
                        } else if (kv_field == 2) {
                            if (thrift_reader_read_string(reader, &meta->kv_values[i]) != 0) return -1;
                        } else {
                            if (thrift_reader_skip_field(reader, kv_type) != 0) return -1;
                        }
                        kv_last_field = kv_field;
                    }
                }
                break;
            }
            case 6: {  // created_by
                if (thrift_reader_read_string(reader, &meta->created_by) != 0) return -1;
                break;
            }
            default:
                if (thrift_reader_skip_field(reader, type) != 0) return -1;
        }
        last_field = field_id;
    }

    return 0;
}

// ============================================================================
// Footer Reading
// ============================================================================

int parquet_read_footer(FILE* file, int64_t file_size, ParquetFileMeta** out_meta) {
    // Parquet footer structure:
    // - Data pages and column chunks
    // - FileMetaData (Thrift-encoded)
    // - 4 bytes: FileMetaData length
    // - 4 bytes: PAR1 magic

    if (file_size < 12) return -1;  // Minimum: 4-byte magic + 4-byte length + 4-byte magic

    // Read footer length and magic
    uint8_t footer_end[8];
    if (fseek(file, file_size - 8, SEEK_SET) != 0) return -1;
    if (fread(footer_end, 1, 8, file) != 8) return -1;

    // Check magic
    if (memcmp(footer_end + 4, "PAR1", 4) != 0) return -1;

    // Get metadata length
    uint32_t metadata_len;
    memcpy(&metadata_len, footer_end, 4);

    if (metadata_len > file_size - 8) return -1;

    // Read metadata
    uint8_t* metadata_buf = malloc(metadata_len);
    if (!metadata_buf) return -1;

    if (fseek(file, file_size - 8 - metadata_len, SEEK_SET) != 0) {
        free(metadata_buf);
        return -1;
    }
    if (fread(metadata_buf, 1, metadata_len, file) != metadata_len) {
        free(metadata_buf);
        return -1;
    }

    // Parse metadata
    ThriftReader reader;
    thrift_reader_init(&reader, metadata_buf, metadata_len);

    *out_meta = calloc(1, sizeof(ParquetFileMeta));
    if (!*out_meta) {
        free(metadata_buf);
        return -1;
    }

    int result = parquet_parse_file_metadata(&reader, *out_meta);
    free(metadata_buf);

    if (result != 0) {
        parquet_file_meta_free(*out_meta);
        *out_meta = NULL;
        return -1;
    }

    return 0;
}

// ============================================================================
// Cleanup Functions
// ============================================================================

void parquet_schema_element_free(ParquetSchemaElement* elem) {
    if (!elem) return;
    free(elem->name);
}

void parquet_column_chunk_meta_free(ParquetColumnChunkMeta* chunk) {
    if (!chunk) return;
    free(chunk->encodings);
    free(chunk->path_in_schema);
}

void parquet_row_group_meta_free(ParquetRowGroupMeta* rg) {
    if (!rg) return;
    if (rg->columns) {
        for (int i = 0; i < rg->num_columns; i++) {
            parquet_column_chunk_meta_free(&rg->columns[i]);
        }
        free(rg->columns);
    }
}

void parquet_file_meta_free(ParquetFileMeta* meta) {
    if (!meta) return;

    if (meta->schema) {
        for (int i = 0; i < meta->num_schema_elements; i++) {
            parquet_schema_element_free(&meta->schema[i]);
        }
        free(meta->schema);
    }

    if (meta->row_groups) {
        for (int i = 0; i < meta->num_row_groups; i++) {
            parquet_row_group_meta_free(&meta->row_groups[i]);
        }
        free(meta->row_groups);
    }

    if (meta->kv_keys) {
        for (int i = 0; i < meta->num_kv; i++) {
            free(meta->kv_keys[i]);
        }
        free(meta->kv_keys);
    }

    if (meta->kv_values) {
        for (int i = 0; i < meta->num_kv; i++) {
            free(meta->kv_values[i]);
        }
        free(meta->kv_values);
    }

    free(meta->created_by);
    free(meta);
}

// ============================================================================
// Schema Conversion (Parquet -> Arrow)
// ============================================================================

static const char* parquet_type_to_arrow_format(ParquetType type, ParquetConvertedType converted) {
    // Handle converted types first
    if (converted >= 0) {
        switch (converted) {
            case PARQUET_CONVERTED_UTF8:
                return "u";  // utf8
            case PARQUET_CONVERTED_INT_8:
                return "c";  // int8
            case PARQUET_CONVERTED_INT_16:
                return "s";  // int16
            case PARQUET_CONVERTED_INT_32:
                return "i";  // int32
            case PARQUET_CONVERTED_INT_64:
                return "l";  // int64
            case PARQUET_CONVERTED_UINT_8:
                return "C";  // uint8
            case PARQUET_CONVERTED_UINT_16:
                return "S";  // uint16
            case PARQUET_CONVERTED_UINT_32:
                return "I";  // uint32
            case PARQUET_CONVERTED_UINT_64:
                return "L";  // uint64
            case PARQUET_CONVERTED_DATE:
                return "tdD";  // date32
            case PARQUET_CONVERTED_TIMESTAMP_MILLIS:
                return "tsm:";  // timestamp milliseconds
            case PARQUET_CONVERTED_TIMESTAMP_MICROS:
                return "tsu:";  // timestamp microseconds
            case PARQUET_CONVERTED_TIME_MILLIS:
                return "ttm";  // time32 milliseconds
            case PARQUET_CONVERTED_TIME_MICROS:
                return "ttu";  // time64 microseconds
            default:
                break;
        }
    }

    // Fall back to physical type
    switch (type) {
        case PARQUET_TYPE_BOOLEAN:
            return "b";  // bool
        case PARQUET_TYPE_INT32:
            return "i";  // int32
        case PARQUET_TYPE_INT64:
            return "l";  // int64
        case PARQUET_TYPE_FLOAT:
            return "f";  // float32
        case PARQUET_TYPE_DOUBLE:
            return "g";  // float64
        case PARQUET_TYPE_BYTE_ARRAY:
            return "u";  // utf8 string
        case PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY:
            return "z";  // binary
        default:
            return "z";  // binary fallback
    }
}

int parquet_schema_to_arrow(const ParquetFileMeta* meta, struct ArrowSchema* out) {
    memset(out, 0, sizeof(*out));

    // Count leaf columns (skip root element)
    int num_columns = 0;
    for (int i = 1; i < meta->num_schema_elements; i++) {
        if (meta->schema[i].num_children == 0) {
            num_columns++;
        }
    }

    // Allocate children
    out->format = "+s";  // struct
    out->name = NULL;
    out->flags = 0;
    out->n_children = num_columns;
    out->children = calloc(num_columns, sizeof(struct ArrowSchema*));
    out->dictionary = NULL;
    out->release = NULL;  // Will be set by caller

    // Fill children
    int col_idx = 0;
    for (int i = 1; i < meta->num_schema_elements && col_idx < num_columns; i++) {
        ParquetSchemaElement* elem = &meta->schema[i];
        if (elem->num_children > 0) continue;  // Skip group elements

        struct ArrowSchema* child = calloc(1, sizeof(struct ArrowSchema));
        out->children[col_idx] = child;

        child->format = parquet_type_to_arrow_format(elem->type, elem->converted_type);
        child->name = elem->name ? strdup(elem->name) : NULL;
        child->flags = (elem->repetition != PARQUET_REPETITION_REQUIRED) ? ARROW_FLAG_NULLABLE : 0;
        child->n_children = 0;
        child->children = NULL;
        child->dictionary = NULL;
        child->release = NULL;

        col_idx++;
    }

    return 0;
}

// ============================================================================
// File Reader Implementation
// ============================================================================

ParquetFileReader* parquet_file_reader_open(const char* path) {
    FILE* file = fopen(path, "rb");
    if (!file) return NULL;

    // Get file size
    if (fseek(file, 0, SEEK_END) != 0) {
        fclose(file);
        return NULL;
    }
    int64_t file_size = ftell(file);
    if (file_size < 0) {
        fclose(file);
        return NULL;
    }

    // Check magic at start
    if (fseek(file, 0, SEEK_SET) != 0) {
        fclose(file);
        return NULL;
    }
    char magic[4];
    if (fread(magic, 1, 4, file) != 4 || memcmp(magic, "PAR1", 4) != 0) {
        fclose(file);
        return NULL;
    }

    // Create reader
    ParquetFileReader* reader = calloc(1, sizeof(ParquetFileReader));
    if (!reader) {
        fclose(file);
        return NULL;
    }

    reader->file = file;
    reader->file_path = strdup(path);
    reader->file_size = file_size;

    // Read and parse footer
    if (parquet_read_footer(file, file_size, &reader->metadata) != 0) {
        fclose(file);
        free(reader->file_path);
        free(reader);
        return NULL;
    }

    return reader;
}

const ParquetFileMeta* parquet_file_reader_get_metadata(ParquetFileReader* reader) {
    return reader ? reader->metadata : NULL;
}

int parquet_file_reader_num_row_groups(ParquetFileReader* reader) {
    return reader && reader->metadata ? reader->metadata->num_row_groups : 0;
}

const ParquetRowGroupMeta* parquet_file_reader_get_row_group(ParquetFileReader* reader, int index) {
    if (!reader || !reader->metadata || index < 0 || index >= reader->metadata->num_row_groups)
        return NULL;
    return &reader->metadata->row_groups[index];
}

void parquet_file_reader_close(ParquetFileReader* reader) {
    if (!reader) return;

    if (reader->file) fclose(reader->file);
    free(reader->file_path);
    parquet_file_meta_free(reader->metadata);
    free(reader->page_buffer);
    free(reader->decompress_buffer);
    free(reader);
}

// ============================================================================
// Data Reading (simplified, uncompressed data pages only)
// ============================================================================

// Helper to read a column into an Arrow array
static int read_column_data(ParquetFileReader* reader,
                            ParquetColumnChunkMeta* col_meta,
                            ParquetSchemaElement* schema_elem,
                            struct ArrowArray** out_array) {
    // Seek to data page
    int64_t offset = col_meta->data_page_offset;
    if (col_meta->dictionary_page_offset > 0 && col_meta->dictionary_page_offset < offset) {
        offset = col_meta->dictionary_page_offset;
    }

    if (fseek(reader->file, offset, SEEK_SET) != 0) return -1;

    // Read page header
    // First, we need to read enough bytes for the header
    uint8_t header_buf[256];
    size_t header_bytes = fread(header_buf, 1, sizeof(header_buf), reader->file);
    if (header_bytes == 0) return -1;

    ThriftReader header_reader;
    thrift_reader_init(&header_reader, header_buf, header_bytes);

    ParquetPageHeader page_header;
    if (parquet_parse_page_header(&header_reader, &page_header) != 0) return -1;

    // Seek back and skip header, then read page data
    size_t header_size = header_reader.pos;
    if (fseek(reader->file, offset + header_size, SEEK_SET) != 0) return -1;

    // Allocate buffer for page data
    size_t data_size = page_header.compressed_page_size;
    uint8_t* page_data = malloc(data_size);
    if (!page_data) return -1;

    if (fread(page_data, 1, data_size, reader->file) != data_size) {
        free(page_data);
        return -1;
    }

    // For now, only handle uncompressed data
    if (col_meta->codec != PARQUET_CODEC_UNCOMPRESSED) {
        // TODO: Add decompression support
        free(page_data);
        return -1;
    }

    // Decode based on physical type
    int num_values = page_header.num_values;
    if (num_values <= 0) num_values = (int)col_meta->num_values;

    struct ArrowArray* array = calloc(1, sizeof(struct ArrowArray));
    if (!array) {
        free(page_data);
        return -1;
    }

    // Skip definition levels for optional columns (simplified)
    size_t data_offset = 0;
    bool is_optional = (schema_elem->repetition == PARQUET_REPETITION_OPTIONAL);

    if (is_optional && page_header.definition_level_encoding == PARQUET_ENCODING_RLE) {
        // Read definition level length (4 bytes)
        if (data_offset + 4 > data_size) {
            free(page_data);
            free(array);
            return -1;
        }
        uint32_t def_level_len;
        memcpy(&def_level_len, page_data + data_offset, 4);
        data_offset += 4 + def_level_len;
    }

    // Decode values
    uint8_t* value_data = page_data + data_offset;
    size_t value_data_size = data_size - data_offset;

    switch (schema_elem->type) {
        case PARQUET_TYPE_INT32: {
            int32_t* values = malloc(num_values * sizeof(int32_t));
            int decoded = decode_plain_int32(value_data, value_data_size, values, num_values);
            if (decoded <= 0) {
                free(values);
                free(page_data);
                free(array);
                return -1;
            }

            // Build Arrow array
            array->length = decoded;
            array->null_count = 0;
            array->offset = 0;
            array->n_buffers = 2;
            array->n_children = 0;
            array->buffers = calloc(2, sizeof(void*));
            array->buffers[0] = NULL;  // No null bitmap
            array->buffers[1] = values;
            array->children = NULL;
            array->dictionary = NULL;
            array->release = NULL;  // Caller should set this
            break;
        }

        case PARQUET_TYPE_INT64: {
            int64_t* values = malloc(num_values * sizeof(int64_t));
            int decoded = decode_plain_int64(value_data, value_data_size, values, num_values);
            if (decoded <= 0) {
                free(values);
                free(page_data);
                free(array);
                return -1;
            }

            array->length = decoded;
            array->null_count = 0;
            array->offset = 0;
            array->n_buffers = 2;
            array->n_children = 0;
            array->buffers = calloc(2, sizeof(void*));
            array->buffers[0] = NULL;
            array->buffers[1] = values;
            array->children = NULL;
            array->dictionary = NULL;
            array->release = NULL;
            break;
        }

        case PARQUET_TYPE_FLOAT: {
            float* values = malloc(num_values * sizeof(float));
            int decoded = decode_plain_float(value_data, value_data_size, values, num_values);
            if (decoded <= 0) {
                free(values);
                free(page_data);
                free(array);
                return -1;
            }

            array->length = decoded;
            array->null_count = 0;
            array->offset = 0;
            array->n_buffers = 2;
            array->n_children = 0;
            array->buffers = calloc(2, sizeof(void*));
            array->buffers[0] = NULL;
            array->buffers[1] = values;
            array->children = NULL;
            array->dictionary = NULL;
            array->release = NULL;
            break;
        }

        case PARQUET_TYPE_DOUBLE: {
            double* values = malloc(num_values * sizeof(double));
            int decoded = decode_plain_double(value_data, value_data_size, values, num_values);
            if (decoded <= 0) {
                free(values);
                free(page_data);
                free(array);
                return -1;
            }

            array->length = decoded;
            array->null_count = 0;
            array->offset = 0;
            array->n_buffers = 2;
            array->n_children = 0;
            array->buffers = calloc(2, sizeof(void*));
            array->buffers[0] = NULL;
            array->buffers[1] = values;
            array->children = NULL;
            array->dictionary = NULL;
            array->release = NULL;
            break;
        }

        case PARQUET_TYPE_BOOLEAN: {
            bool* values = malloc(num_values * sizeof(bool));
            int decoded = decode_plain_boolean(value_data, value_data_size, values, num_values);
            if (decoded <= 0) {
                free(values);
                free(page_data);
                free(array);
                return -1;
            }

            // Convert to bit-packed format
            size_t bit_bytes = (decoded + 7) / 8;
            uint8_t* bit_data = calloc(bit_bytes, 1);
            for (int i = 0; i < decoded; i++) {
                if (values[i]) {
                    bit_data[i / 8] |= (1 << (i % 8));
                }
            }
            free(values);

            array->length = decoded;
            array->null_count = 0;
            array->offset = 0;
            array->n_buffers = 2;
            array->n_children = 0;
            array->buffers = calloc(2, sizeof(void*));
            array->buffers[0] = NULL;
            array->buffers[1] = bit_data;
            array->children = NULL;
            array->dictionary = NULL;
            array->release = NULL;
            break;
        }

        case PARQUET_TYPE_BYTE_ARRAY: {
            int32_t* offsets = NULL;
            uint8_t* values = NULL;
            size_t values_len = 0;
            int decoded = decode_plain_byte_array(value_data, value_data_size,
                                                   &offsets, &values, num_values, &values_len);
            if (decoded < 0) {
                free(page_data);
                free(array);
                return -1;
            }

            array->length = decoded;
            array->null_count = 0;
            array->offset = 0;
            array->n_buffers = 3;
            array->n_children = 0;
            array->buffers = calloc(3, sizeof(void*));
            array->buffers[0] = NULL;     // No null bitmap
            array->buffers[1] = offsets;  // Offset buffer
            array->buffers[2] = values;   // Data buffer
            array->children = NULL;
            array->dictionary = NULL;
            array->release = NULL;
            break;
        }

        default:
            free(page_data);
            free(array);
            return -1;
    }

    free(page_data);
    *out_array = array;
    return 0;
}

// Simplified row group reader
struct ArrowArrayStream* parquet_file_reader_read_row_group(ParquetFileReader* reader, int row_group_index) {
    if (!reader || !reader->metadata) return NULL;
    if (row_group_index < 0 || row_group_index >= reader->metadata->num_row_groups) return NULL;

    ParquetRowGroupMeta* rg = &reader->metadata->row_groups[row_group_index];

    // For now, just return a placeholder
    // Full implementation would read all columns and create a proper stream
    struct ArrowArrayStream* stream = calloc(1, sizeof(struct ArrowArrayStream));
    if (!stream) return NULL;

    // TODO: Implement proper streaming of row group data
    // This would involve:
    // 1. Creating Arrow schema from Parquet schema
    // 2. Reading each column's data pages
    // 3. Combining into record batches
    // 4. Wrapping in a stream interface

    return stream;
}

struct ArrowArrayStream* parquet_file_reader_read_all(ParquetFileReader* reader) {
    if (!reader || !reader->metadata) return NULL;

    // Read all row groups and combine
    struct ArrowArrayStream* stream = calloc(1, sizeof(struct ArrowArrayStream));
    if (!stream) return NULL;

    // TODO: Implement full file reading
    return stream;
}

struct ArrowArrayStream* parquet_file_reader_read_columns(ParquetFileReader* reader,
                                                           int row_group_index,
                                                           int* column_indices,
                                                           int num_columns) {
    if (!reader || !reader->metadata) return NULL;

    // TODO: Implement column projection
    return NULL;
}
