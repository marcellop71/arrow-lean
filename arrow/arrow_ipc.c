/*
 * Arrow IPC Serialization/Deserialization Implementation
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "arrow_ipc.h"
#include "arrow_c_abi.h"

/* ============================================================================
 * Buffer Operations
 * ============================================================================ */

void arrow_ipc_buffer_init(ArrowIPCBuffer* buf) {
    buf->data = NULL;
    buf->size = 0;
    buf->capacity = 0;
}

void arrow_ipc_buffer_free(ArrowIPCBuffer* buf) {
    if (buf->data) {
        free(buf->data);
        buf->data = NULL;
    }
    buf->size = 0;
    buf->capacity = 0;
}

int arrow_ipc_buffer_reserve(ArrowIPCBuffer* buf, size_t additional) {
    size_t needed = buf->size + additional;
    if (needed <= buf->capacity) {
        return ARROW_IPC_OK;
    }

    /* Grow by at least 2x or to needed size */
    size_t new_capacity = buf->capacity * 2;
    if (new_capacity < needed) {
        new_capacity = needed;
    }
    if (new_capacity < 256) {
        new_capacity = 256;
    }

    uint8_t* new_data = (uint8_t*)realloc(buf->data, new_capacity);
    if (!new_data) {
        return ARROW_IPC_ERR_ALLOC;
    }

    buf->data = new_data;
    buf->capacity = new_capacity;
    return ARROW_IPC_OK;
}

int arrow_ipc_buffer_write(ArrowIPCBuffer* buf, const void* data, size_t len) {
    if (len == 0) return ARROW_IPC_OK;

    int rc = arrow_ipc_buffer_reserve(buf, len);
    if (rc != ARROW_IPC_OK) return rc;

    memcpy(buf->data + buf->size, data, len);
    buf->size += len;
    return ARROW_IPC_OK;
}

int arrow_ipc_buffer_write_u32(ArrowIPCBuffer* buf, uint32_t value) {
    /* Little-endian */
    uint8_t bytes[4];
    bytes[0] = (uint8_t)(value & 0xFF);
    bytes[1] = (uint8_t)((value >> 8) & 0xFF);
    bytes[2] = (uint8_t)((value >> 16) & 0xFF);
    bytes[3] = (uint8_t)((value >> 24) & 0xFF);
    return arrow_ipc_buffer_write(buf, bytes, 4);
}

int arrow_ipc_buffer_write_u64(ArrowIPCBuffer* buf, uint64_t value) {
    /* Little-endian */
    uint8_t bytes[8];
    for (int i = 0; i < 8; i++) {
        bytes[i] = (uint8_t)((value >> (i * 8)) & 0xFF);
    }
    return arrow_ipc_buffer_write(buf, bytes, 8);
}

int arrow_ipc_buffer_write_i64(ArrowIPCBuffer* buf, int64_t value) {
    return arrow_ipc_buffer_write_u64(buf, (uint64_t)value);
}

/* Read helpers */
static uint32_t read_u32(const uint8_t* data) {
    return (uint32_t)data[0] |
           ((uint32_t)data[1] << 8) |
           ((uint32_t)data[2] << 16) |
           ((uint32_t)data[3] << 24);
}

static uint64_t read_u64(const uint8_t* data) {
    uint64_t value = 0;
    for (int i = 0; i < 8; i++) {
        value |= ((uint64_t)data[i] << (i * 8));
    }
    return value;
}

static int64_t read_i64(const uint8_t* data) {
    return (int64_t)read_u64(data);
}

/* ============================================================================
 * Format Helpers
 * ============================================================================ */

int arrow_ipc_get_buffer_count(const char* format) {
    if (!format || !format[0]) return -1;

    switch (format[0]) {
        case 'n': return 0;  /* null - no buffers */
        case 'b': return 2;  /* boolean: validity + data */
        case 'c': case 's': case 'i': case 'l':  /* signed int */
        case 'C': case 'S': case 'I': case 'L':  /* unsigned int */
        case 'e': case 'f': case 'g':            /* float */
            return 2;  /* validity + data */
        case 'u': case 'U':  /* utf8 string */
        case 'z': case 'Z':  /* binary */
            return 3;  /* validity + offsets + data */
        case '+':
            if (format[1] == 's') return 1;  /* struct: just validity */
            if (format[1] == 'l') return 2;  /* list: validity + offsets */
            return -1;
        case 't':
            /* timestamp, date, time, duration */
            return 2;  /* validity + data */
        default:
            return 2;  /* default assumption */
    }
}

int arrow_ipc_get_element_size(const char* format) {
    if (!format || !format[0]) return -1;

    switch (format[0]) {
        case 'n': return 0;   /* null */
        case 'b': return 1;   /* boolean (1 bit, stored as bytes) */
        case 'c': case 'C': return 1;   /* int8/uint8 */
        case 's': case 'S': return 2;   /* int16/uint16 */
        case 'i': case 'I': return 4;   /* int32/uint32 */
        case 'l': case 'L': return 8;   /* int64/uint64 */
        case 'e': return 2;   /* float16 */
        case 'f': return 4;   /* float32 */
        case 'g': return 8;   /* float64 */
        case 'u': case 'U': case 'z': case 'Z':
            return 0;  /* variable-width */
        case 't':
            /* Timestamps are 64-bit */
            if (format[1] == 's' || format[1] == 'd') return 8;
            return 4;  /* date32, time32 */
        default:
            return 0;
    }
}

/* ============================================================================
 * Schema Serialization
 * ============================================================================ */

static int serialize_schema_internal(ArrowIPCBuffer* buf, const struct ArrowSchema* schema) {
    int rc;

    /* Format string */
    size_t format_len = schema->format ? strlen(schema->format) : 0;
    rc = arrow_ipc_buffer_write_u32(buf, (uint32_t)format_len);
    if (rc != ARROW_IPC_OK) return rc;
    if (format_len > 0) {
        rc = arrow_ipc_buffer_write(buf, schema->format, format_len);
        if (rc != ARROW_IPC_OK) return rc;
    }

    /* Name (optional) */
    size_t name_len = schema->name ? strlen(schema->name) : 0;
    rc = arrow_ipc_buffer_write_u32(buf, (uint32_t)name_len);
    if (rc != ARROW_IPC_OK) return rc;
    if (name_len > 0) {
        rc = arrow_ipc_buffer_write(buf, schema->name, name_len);
        if (rc != ARROW_IPC_OK) return rc;
    }

    /* Flags */
    rc = arrow_ipc_buffer_write_i64(buf, schema->flags);
    if (rc != ARROW_IPC_OK) return rc;

    /* Number of children */
    rc = arrow_ipc_buffer_write_i64(buf, schema->n_children);
    if (rc != ARROW_IPC_OK) return rc;

    /* Serialize children recursively */
    for (int64_t i = 0; i < schema->n_children; i++) {
        if (schema->children && schema->children[i]) {
            rc = serialize_schema_internal(buf, schema->children[i]);
            if (rc != ARROW_IPC_OK) return rc;
        }
    }

    return ARROW_IPC_OK;
}

int arrow_ipc_serialize_schema(
    const struct ArrowSchema* schema,
    uint8_t** out_data,
    size_t* out_size
) {
    if (!schema || !out_data || !out_size) {
        return ARROW_IPC_ERR_NULL;
    }

    ArrowIPCBuffer buf;
    arrow_ipc_buffer_init(&buf);

    int rc;

    /* Magic number */
    rc = arrow_ipc_buffer_write_u32(&buf, ARROW_IPC_MAGIC_SCHEMA);
    if (rc != ARROW_IPC_OK) goto error;

    /* Version */
    rc = arrow_ipc_buffer_write_u32(&buf, ARROW_IPC_VERSION);
    if (rc != ARROW_IPC_OK) goto error;

    /* Schema content */
    rc = serialize_schema_internal(&buf, schema);
    if (rc != ARROW_IPC_OK) goto error;

    *out_data = buf.data;
    *out_size = buf.size;
    return ARROW_IPC_OK;

error:
    arrow_ipc_buffer_free(&buf);
    return rc;
}

/* ============================================================================
 * Schema Deserialization
 * ============================================================================ */

/* Forward declaration for release callback */
static void release_deserialized_schema(struct ArrowSchema* schema);

static int deserialize_schema_internal(
    const uint8_t* data,
    size_t size,
    struct ArrowSchema* out_schema,
    size_t* bytes_consumed
) {
    size_t offset = 0;

    /* Initialize output */
    memset(out_schema, 0, sizeof(struct ArrowSchema));

    /* Format string length */
    if (offset + 4 > size) return ARROW_IPC_ERR_TRUNCATE;
    uint32_t format_len = read_u32(data + offset);
    offset += 4;

    /* Format string */
    if (offset + format_len > size) return ARROW_IPC_ERR_TRUNCATE;
    if (format_len > 0) {
        char* format = (char*)malloc(format_len + 1);
        if (!format) return ARROW_IPC_ERR_ALLOC;
        memcpy(format, data + offset, format_len);
        format[format_len] = '\0';
        out_schema->format = format;
    } else {
        out_schema->format = NULL;
    }
    offset += format_len;

    /* Name length */
    if (offset + 4 > size) goto error;
    uint32_t name_len = read_u32(data + offset);
    offset += 4;

    /* Name */
    if (offset + name_len > size) goto error;
    if (name_len > 0) {
        char* name = (char*)malloc(name_len + 1);
        if (!name) goto error;
        memcpy(name, data + offset, name_len);
        name[name_len] = '\0';
        out_schema->name = name;
    } else {
        out_schema->name = NULL;
    }
    offset += name_len;

    /* Flags */
    if (offset + 8 > size) goto error;
    out_schema->flags = read_i64(data + offset);
    offset += 8;

    /* Number of children */
    if (offset + 8 > size) goto error;
    out_schema->n_children = read_i64(data + offset);
    offset += 8;

    /* Allocate and deserialize children */
    if (out_schema->n_children > 0) {
        out_schema->children = (struct ArrowSchema**)calloc(
            out_schema->n_children, sizeof(struct ArrowSchema*));
        if (!out_schema->children) goto error;

        for (int64_t i = 0; i < out_schema->n_children; i++) {
            out_schema->children[i] = (struct ArrowSchema*)calloc(1, sizeof(struct ArrowSchema));
            if (!out_schema->children[i]) goto error;

            size_t child_bytes = 0;
            int rc = deserialize_schema_internal(
                data + offset, size - offset,
                out_schema->children[i], &child_bytes);
            if (rc != ARROW_IPC_OK) goto error;
            offset += child_bytes;
        }
    }

    /* Set release callback */
    out_schema->release = release_deserialized_schema;
    out_schema->dictionary = NULL;
    out_schema->metadata = NULL;
    out_schema->private_data = NULL;

    if (bytes_consumed) *bytes_consumed = offset;
    return ARROW_IPC_OK;

error:
    release_deserialized_schema(out_schema);
    return ARROW_IPC_ERR_TRUNCATE;
}

static void release_deserialized_schema(struct ArrowSchema* schema) {
    if (!schema || !schema->release) return;

    if (schema->format) {
        free((void*)schema->format);
        schema->format = NULL;
    }
    if (schema->name) {
        free((void*)schema->name);
        schema->name = NULL;
    }
    if (schema->children) {
        for (int64_t i = 0; i < schema->n_children; i++) {
            if (schema->children[i]) {
                if (schema->children[i]->release) {
                    schema->children[i]->release(schema->children[i]);
                }
                free(schema->children[i]);
            }
        }
        free(schema->children);
        schema->children = NULL;
    }
    schema->release = NULL;
}

int arrow_ipc_deserialize_schema(
    const uint8_t* data,
    size_t size,
    struct ArrowSchema* out_schema,
    size_t* bytes_read
) {
    if (!data || !out_schema) {
        return ARROW_IPC_ERR_NULL;
    }

    size_t offset = 0;

    /* Magic number */
    if (offset + 4 > size) return ARROW_IPC_ERR_TRUNCATE;
    uint32_t magic = read_u32(data + offset);
    if (magic != ARROW_IPC_MAGIC_SCHEMA) return ARROW_IPC_ERR_FORMAT;
    offset += 4;

    /* Version */
    if (offset + 4 > size) return ARROW_IPC_ERR_TRUNCATE;
    uint32_t version = read_u32(data + offset);
    if (version != ARROW_IPC_VERSION) return ARROW_IPC_ERR_VERSION;
    offset += 4;

    /* Deserialize schema content */
    size_t content_bytes = 0;
    int rc = deserialize_schema_internal(data + offset, size - offset, out_schema, &content_bytes);
    if (rc != ARROW_IPC_OK) return rc;
    offset += content_bytes;

    if (bytes_read) *bytes_read = offset;
    return ARROW_IPC_OK;
}

/* ============================================================================
 * Array Serialization
 * ============================================================================ */

static int serialize_array_internal(
    ArrowIPCBuffer* buf,
    const struct ArrowArray* array,
    const struct ArrowSchema* schema
) {
    int rc;

    /* Array metadata */
    rc = arrow_ipc_buffer_write_i64(buf, array->length);
    if (rc != ARROW_IPC_OK) return rc;

    rc = arrow_ipc_buffer_write_i64(buf, array->null_count);
    if (rc != ARROW_IPC_OK) return rc;

    rc = arrow_ipc_buffer_write_i64(buf, array->offset);
    if (rc != ARROW_IPC_OK) return rc;

    rc = arrow_ipc_buffer_write_i64(buf, array->n_buffers);
    if (rc != ARROW_IPC_OK) return rc;

    rc = arrow_ipc_buffer_write_i64(buf, array->n_children);
    if (rc != ARROW_IPC_OK) return rc;

    /* Calculate buffer sizes based on type */
    int element_size = arrow_ipc_get_element_size(schema->format);

    /* Serialize each buffer */
    for (int64_t i = 0; i < array->n_buffers; i++) {
        size_t buffer_size = 0;

        if (array->buffers && array->buffers[i]) {
            /* Determine buffer size based on buffer index and type */
            if (i == 0) {
                /* Validity bitmap: (length + 7) / 8 bytes */
                buffer_size = (array->length + 7) / 8;
            } else if (schema->format[0] == 'u' || schema->format[0] == 'U' ||
                       schema->format[0] == 'z' || schema->format[0] == 'Z') {
                /* Variable-width: buffer 1 is offsets, buffer 2 is data */
                if (i == 1) {
                    /* Offsets: (length + 1) * 4 bytes for 32-bit offsets */
                    buffer_size = (array->length + 1) * 4;
                } else if (i == 2) {
                    /* Data buffer: read size from last offset */
                    const int32_t* offsets = (const int32_t*)array->buffers[1];
                    if (offsets) {
                        buffer_size = offsets[array->length];
                    }
                }
            } else if (schema->format[0] == 'b') {
                /* Boolean: data is also a bitmap */
                buffer_size = (array->length + 7) / 8;
            } else if (element_size > 0) {
                /* Fixed-width: length * element_size */
                buffer_size = array->length * element_size;
            }
        }

        /* Write buffer size */
        rc = arrow_ipc_buffer_write_u64(buf, (uint64_t)buffer_size);
        if (rc != ARROW_IPC_OK) return rc;

        /* Write buffer data */
        if (buffer_size > 0 && array->buffers && array->buffers[i]) {
            rc = arrow_ipc_buffer_write(buf, array->buffers[i], buffer_size);
            if (rc != ARROW_IPC_OK) return rc;
        }
    }

    /* Serialize children */
    for (int64_t i = 0; i < array->n_children; i++) {
        if (array->children && array->children[i] &&
            schema->children && schema->children[i]) {
            rc = serialize_array_internal(buf, array->children[i], schema->children[i]);
            if (rc != ARROW_IPC_OK) return rc;
        }
    }

    return ARROW_IPC_OK;
}

int arrow_ipc_serialize_array(
    const struct ArrowArray* array,
    const struct ArrowSchema* schema,
    uint8_t** out_data,
    size_t* out_size
) {
    if (!array || !schema || !out_data || !out_size) {
        return ARROW_IPC_ERR_NULL;
    }

    ArrowIPCBuffer buf;
    arrow_ipc_buffer_init(&buf);

    int rc;

    /* Magic number */
    rc = arrow_ipc_buffer_write_u32(&buf, ARROW_IPC_MAGIC_ARRAY);
    if (rc != ARROW_IPC_OK) goto error;

    /* Version */
    rc = arrow_ipc_buffer_write_u32(&buf, ARROW_IPC_VERSION);
    if (rc != ARROW_IPC_OK) goto error;

    /* Array content */
    rc = serialize_array_internal(&buf, array, schema);
    if (rc != ARROW_IPC_OK) goto error;

    *out_data = buf.data;
    *out_size = buf.size;
    return ARROW_IPC_OK;

error:
    arrow_ipc_buffer_free(&buf);
    return rc;
}

/* ============================================================================
 * Array Deserialization
 * ============================================================================ */

/* Forward declaration */
static void release_deserialized_array(struct ArrowArray* array);

static int deserialize_array_internal(
    const uint8_t* data,
    size_t size,
    const struct ArrowSchema* schema,
    struct ArrowArray* out_array,
    size_t* bytes_consumed
) {
    size_t offset = 0;

    /* Initialize output */
    memset(out_array, 0, sizeof(struct ArrowArray));

    /* Read metadata */
    if (offset + 8 > size) return ARROW_IPC_ERR_TRUNCATE;
    out_array->length = read_i64(data + offset);
    offset += 8;

    if (offset + 8 > size) return ARROW_IPC_ERR_TRUNCATE;
    out_array->null_count = read_i64(data + offset);
    offset += 8;

    if (offset + 8 > size) return ARROW_IPC_ERR_TRUNCATE;
    out_array->offset = read_i64(data + offset);
    offset += 8;

    if (offset + 8 > size) return ARROW_IPC_ERR_TRUNCATE;
    out_array->n_buffers = read_i64(data + offset);
    offset += 8;

    if (offset + 8 > size) return ARROW_IPC_ERR_TRUNCATE;
    out_array->n_children = read_i64(data + offset);
    offset += 8;

    /* Allocate and read buffers */
    if (out_array->n_buffers > 0) {
        out_array->buffers = (const void**)calloc(out_array->n_buffers, sizeof(void*));
        if (!out_array->buffers) goto error;

        for (int64_t i = 0; i < out_array->n_buffers; i++) {
            /* Read buffer size */
            if (offset + 8 > size) goto error;
            uint64_t buffer_size = read_u64(data + offset);
            offset += 8;

            /* Read buffer data */
            if (buffer_size > 0) {
                if (offset + buffer_size > size) goto error;

                void* buffer = malloc(buffer_size);
                if (!buffer) goto error;
                memcpy(buffer, data + offset, buffer_size);
                out_array->buffers[i] = buffer;
                offset += buffer_size;
            } else {
                out_array->buffers[i] = NULL;
            }
        }
    }

    /* Deserialize children */
    if (out_array->n_children > 0) {
        out_array->children = (struct ArrowArray**)calloc(
            out_array->n_children, sizeof(struct ArrowArray*));
        if (!out_array->children) goto error;

        for (int64_t i = 0; i < out_array->n_children; i++) {
            out_array->children[i] = (struct ArrowArray*)calloc(1, sizeof(struct ArrowArray));
            if (!out_array->children[i]) goto error;

            const struct ArrowSchema* child_schema = NULL;
            if (schema && schema->children && i < schema->n_children) {
                child_schema = schema->children[i];
            }

            size_t child_bytes = 0;
            int rc = deserialize_array_internal(
                data + offset, size - offset,
                child_schema, out_array->children[i], &child_bytes);
            if (rc != ARROW_IPC_OK) goto error;
            offset += child_bytes;
        }
    }

    /* Set release callback */
    out_array->release = release_deserialized_array;
    out_array->dictionary = NULL;
    out_array->private_data = NULL;

    if (bytes_consumed) *bytes_consumed = offset;
    return ARROW_IPC_OK;

error:
    release_deserialized_array(out_array);
    return ARROW_IPC_ERR_ALLOC;
}

static void release_deserialized_array(struct ArrowArray* array) {
    if (!array || !array->release) return;

    /* Free buffers */
    if (array->buffers) {
        for (int64_t i = 0; i < array->n_buffers; i++) {
            if (array->buffers[i]) {
                free((void*)array->buffers[i]);
            }
        }
        free((void*)array->buffers);
        array->buffers = NULL;
    }

    /* Free children */
    if (array->children) {
        for (int64_t i = 0; i < array->n_children; i++) {
            if (array->children[i]) {
                if (array->children[i]->release) {
                    array->children[i]->release(array->children[i]);
                }
                free(array->children[i]);
            }
        }
        free(array->children);
        array->children = NULL;
    }

    array->release = NULL;
}

int arrow_ipc_deserialize_array(
    const uint8_t* data,
    size_t size,
    const struct ArrowSchema* schema,
    struct ArrowArray* out_array,
    size_t* bytes_read
) {
    if (!data || !out_array) {
        return ARROW_IPC_ERR_NULL;
    }

    size_t offset = 0;

    /* Magic number */
    if (offset + 4 > size) return ARROW_IPC_ERR_TRUNCATE;
    uint32_t magic = read_u32(data + offset);
    if (magic != ARROW_IPC_MAGIC_ARRAY) return ARROW_IPC_ERR_FORMAT;
    offset += 4;

    /* Version */
    if (offset + 4 > size) return ARROW_IPC_ERR_TRUNCATE;
    uint32_t version = read_u32(data + offset);
    if (version != ARROW_IPC_VERSION) return ARROW_IPC_ERR_VERSION;
    offset += 4;

    /* Deserialize array content */
    size_t content_bytes = 0;
    int rc = deserialize_array_internal(data + offset, size - offset, schema, out_array, &content_bytes);
    if (rc != ARROW_IPC_OK) return rc;
    offset += content_bytes;

    if (bytes_read) *bytes_read = offset;
    return ARROW_IPC_OK;
}

/* ============================================================================
 * RecordBatch Serialization
 * ============================================================================ */

int arrow_ipc_serialize_batch(
    const struct ArrowSchema* schema,
    const struct ArrowArray* array,
    uint8_t** out_data,
    size_t* out_size
) {
    if (!schema || !array || !out_data || !out_size) {
        return ARROW_IPC_ERR_NULL;
    }

    ArrowIPCBuffer buf;
    arrow_ipc_buffer_init(&buf);

    int rc;

    /* Magic number for batch */
    rc = arrow_ipc_buffer_write_u32(&buf, ARROW_IPC_MAGIC_BATCH);
    if (rc != ARROW_IPC_OK) goto error;

    /* Version */
    rc = arrow_ipc_buffer_write_u32(&buf, ARROW_IPC_VERSION);
    if (rc != ARROW_IPC_OK) goto error;

    /* Serialize schema (inline, without header) */
    rc = serialize_schema_internal(&buf, schema);
    if (rc != ARROW_IPC_OK) goto error;

    /* Serialize array (inline, without header) */
    rc = serialize_array_internal(&buf, array, schema);
    if (rc != ARROW_IPC_OK) goto error;

    *out_data = buf.data;
    *out_size = buf.size;
    return ARROW_IPC_OK;

error:
    arrow_ipc_buffer_free(&buf);
    return rc;
}

int arrow_ipc_deserialize_batch(
    const uint8_t* data,
    size_t size,
    struct ArrowSchema* out_schema,
    struct ArrowArray* out_array
) {
    if (!data || !out_schema || !out_array) {
        return ARROW_IPC_ERR_NULL;
    }

    size_t offset = 0;

    /* Magic number */
    if (offset + 4 > size) return ARROW_IPC_ERR_TRUNCATE;
    uint32_t magic = read_u32(data + offset);
    if (magic != ARROW_IPC_MAGIC_BATCH) return ARROW_IPC_ERR_FORMAT;
    offset += 4;

    /* Version */
    if (offset + 4 > size) return ARROW_IPC_ERR_TRUNCATE;
    uint32_t version = read_u32(data + offset);
    if (version != ARROW_IPC_VERSION) return ARROW_IPC_ERR_VERSION;
    offset += 4;

    /* Deserialize schema */
    size_t schema_bytes = 0;
    int rc = deserialize_schema_internal(data + offset, size - offset, out_schema, &schema_bytes);
    if (rc != ARROW_IPC_OK) return rc;
    offset += schema_bytes;

    /* Deserialize array */
    size_t array_bytes = 0;
    rc = deserialize_array_internal(data + offset, size - offset, out_schema, out_array, &array_bytes);
    if (rc != ARROW_IPC_OK) {
        release_deserialized_schema(out_schema);
        return rc;
    }

    return ARROW_IPC_OK;
}
