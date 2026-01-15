/*
 * Arrow IPC Serialization/Deserialization
 *
 * Provides a minimal binary format for serializing Arrow arrays and schemas
 * for storage (e.g., in Redis) and later retrieval.
 *
 * This is a simplified format optimized for same-application round-trips,
 * not full Arrow IPC interoperability with other implementations.
 *
 * Format overview:
 * - RecordBatch: magic + version + schema + array
 * - Schema: format string + name + flags + children (recursive)
 * - Array: length + null_count + offset + buffers + children (recursive)
 */

#ifndef ARROW_IPC_H
#define ARROW_IPC_H

#include <stdint.h>
#include <stddef.h>
#include "arrow_c_abi.h"

/* Magic numbers for format identification */
#define ARROW_IPC_MAGIC_SCHEMA  0x53525241  /* "ARRS" */
#define ARROW_IPC_MAGIC_ARRAY   0x41525241  /* "ARRA" */
#define ARROW_IPC_MAGIC_BATCH   0x42525241  /* "ARRB" */
#define ARROW_IPC_VERSION       1

/* Result codes */
#define ARROW_IPC_OK            0
#define ARROW_IPC_ERR_ALLOC    -1
#define ARROW_IPC_ERR_FORMAT   -2
#define ARROW_IPC_ERR_VERSION  -3
#define ARROW_IPC_ERR_TRUNCATE -4
#define ARROW_IPC_ERR_NULL     -5

/* Buffer for building serialized data */
typedef struct {
    uint8_t* data;
    size_t size;
    size_t capacity;
} ArrowIPCBuffer;

/* Initialize a buffer */
void arrow_ipc_buffer_init(ArrowIPCBuffer* buf);

/* Free buffer resources */
void arrow_ipc_buffer_free(ArrowIPCBuffer* buf);

/* Reserve capacity in buffer */
int arrow_ipc_buffer_reserve(ArrowIPCBuffer* buf, size_t additional);

/* Write raw bytes to buffer */
int arrow_ipc_buffer_write(ArrowIPCBuffer* buf, const void* data, size_t len);

/* Write integer types (little-endian) */
int arrow_ipc_buffer_write_u32(ArrowIPCBuffer* buf, uint32_t value);
int arrow_ipc_buffer_write_u64(ArrowIPCBuffer* buf, uint64_t value);
int arrow_ipc_buffer_write_i64(ArrowIPCBuffer* buf, int64_t value);

/*
 * Serialize an ArrowSchema to binary format
 *
 * @param schema The schema to serialize (must not be NULL)
 * @param out_data Output pointer to allocated buffer (caller must free)
 * @param out_size Output size of the serialized data
 * @return ARROW_IPC_OK on success, error code otherwise
 */
int arrow_ipc_serialize_schema(
    const struct ArrowSchema* schema,
    uint8_t** out_data,
    size_t* out_size
);

/*
 * Deserialize an ArrowSchema from binary format
 *
 * @param data Input buffer containing serialized schema
 * @param size Size of input buffer
 * @param out_schema Output schema (will be initialized)
 * @param bytes_read Output number of bytes consumed (can be NULL)
 * @return ARROW_IPC_OK on success, error code otherwise
 */
int arrow_ipc_deserialize_schema(
    const uint8_t* data,
    size_t size,
    struct ArrowSchema* out_schema,
    size_t* bytes_read
);

/*
 * Serialize an ArrowArray to binary format
 *
 * @param array The array to serialize (must not be NULL)
 * @param schema The schema describing the array type (for buffer interpretation)
 * @param out_data Output pointer to allocated buffer (caller must free)
 * @param out_size Output size of the serialized data
 * @return ARROW_IPC_OK on success, error code otherwise
 */
int arrow_ipc_serialize_array(
    const struct ArrowArray* array,
    const struct ArrowSchema* schema,
    uint8_t** out_data,
    size_t* out_size
);

/*
 * Deserialize an ArrowArray from binary format
 *
 * @param data Input buffer containing serialized array
 * @param size Size of input buffer
 * @param schema The schema describing the expected array type
 * @param out_array Output array (will be initialized)
 * @param bytes_read Output number of bytes consumed (can be NULL)
 * @return ARROW_IPC_OK on success, error code otherwise
 */
int arrow_ipc_deserialize_array(
    const uint8_t* data,
    size_t size,
    const struct ArrowSchema* schema,
    struct ArrowArray* out_array,
    size_t* bytes_read
);

/*
 * Serialize a RecordBatch (schema + array) to binary format
 *
 * @param schema The schema
 * @param array The array
 * @param out_data Output pointer to allocated buffer (caller must free)
 * @param out_size Output size of the serialized data
 * @return ARROW_IPC_OK on success, error code otherwise
 */
int arrow_ipc_serialize_batch(
    const struct ArrowSchema* schema,
    const struct ArrowArray* array,
    uint8_t** out_data,
    size_t* out_size
);

/*
 * Deserialize a RecordBatch from binary format
 *
 * @param data Input buffer
 * @param size Size of input buffer
 * @param out_schema Output schema
 * @param out_array Output array
 * @return ARROW_IPC_OK on success, error code otherwise
 */
int arrow_ipc_deserialize_batch(
    const uint8_t* data,
    size_t size,
    struct ArrowSchema* out_schema,
    struct ArrowArray* out_array
);

/*
 * Get the number of buffers for a given Arrow format string
 *
 * @param format Arrow format string (e.g., "l" for int64)
 * @return Number of buffers, or -1 if unknown format
 */
int arrow_ipc_get_buffer_count(const char* format);

/*
 * Get the element size in bytes for fixed-width types
 *
 * @param format Arrow format string
 * @return Element size in bytes, or 0 for variable-width types, -1 for unknown
 */
int arrow_ipc_get_element_size(const char* format);

#endif /* ARROW_IPC_H */
