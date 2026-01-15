/*
 * Lean FFI Wrapper for Arrow IPC Serialization
 *
 * Provides Lean-callable functions for serializing/deserializing
 * Arrow schemas, arrays, and record batches.
 */

#include <lean/lean.h>
#include <stdlib.h>
#include <string.h>
#include "arrow_c_abi.h"
#include "arrow_wrapper.h"
#include "arrow_ipc.h"

/* ============================================================================
 * Helper Functions (matching lean_arrow_wrapper.c patterns)
 * ============================================================================ */

static lean_obj_res lean_mk_option_none_ipc(void) {
    return lean_alloc_ctor(0, 0, 0);
}

static lean_obj_res lean_mk_option_some_ipc(lean_obj_arg value) {
    lean_object* option = lean_alloc_ctor(1, 1, 0);
    lean_ctor_set(option, 0, value);
    return option;
}

static lean_object* make_ipc_error(int error_code) {
    const char* msg;
    switch (error_code) {
        case ARROW_IPC_ERR_ALLOC:
            msg = "IPC allocation error";
            break;
        case ARROW_IPC_ERR_FORMAT:
            msg = "IPC format error: invalid magic number";
            break;
        case ARROW_IPC_ERR_VERSION:
            msg = "IPC version error: unsupported version";
            break;
        case ARROW_IPC_ERR_TRUNCATE:
            msg = "IPC truncation error: unexpected end of data";
            break;
        case ARROW_IPC_ERR_NULL:
            msg = "IPC null error: null pointer argument";
            break;
        default:
            msg = "IPC unknown error";
            break;
    }
    return lean_mk_io_user_error(lean_mk_string(msg));
}

/* ============================================================================
 * Schema Serialization
 * ============================================================================ */

/*
 * Serialize an ArrowSchema to ByteArray
 *
 * @extern "lean_arrow_ipc_serialize_schema"
 * opaque serializeSchemaRaw : ArrowSchemaPtr.type → IO ByteArray
 */
LEAN_EXPORT lean_obj_res lean_arrow_ipc_serialize_schema(
    b_lean_obj_arg schema_ptr_obj,
    lean_obj_arg w
) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Null schema pointer")));
    }

    uint8_t* data = NULL;
    size_t size = 0;

    int rc = arrow_ipc_serialize_schema(schema, &data, &size);
    if (rc != ARROW_IPC_OK) {
        return lean_io_result_mk_error(make_ipc_error(rc));
    }

    /* Create Lean ByteArray from the serialized data */
    lean_object* byte_array = lean_alloc_sarray(1, size, size);
    memcpy(lean_sarray_cptr(byte_array), data, size);
    free(data);

    return lean_io_result_mk_ok(byte_array);
}

/*
 * Deserialize a ByteArray to ArrowSchema
 *
 * @extern "lean_arrow_ipc_deserialize_schema"
 * opaque deserializeSchemaRaw : @& ByteArray → IO (Option ArrowSchemaPtr.type)
 */
LEAN_EXPORT lean_obj_res lean_arrow_ipc_deserialize_schema(
    b_lean_obj_arg byte_array,
    lean_obj_arg w
) {
    const uint8_t* data = lean_sarray_cptr(byte_array);
    size_t size = lean_sarray_size(byte_array);

    struct ArrowSchema* schema = (struct ArrowSchema*)calloc(1, sizeof(struct ArrowSchema));
    if (!schema) {
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Failed to allocate schema")));
    }

    int rc = arrow_ipc_deserialize_schema(data, size, schema, NULL);
    if (rc != ARROW_IPC_OK) {
        free(schema);
        /* Return none on deserialization failure */
        return lean_io_result_mk_ok(lean_mk_option_none_ipc());
    }

    /* Box the pointer as usize (matching existing pattern) */
    lean_object* schema_obj = lean_box_usize((uintptr_t)schema);
    return lean_io_result_mk_ok(lean_mk_option_some_ipc(schema_obj));
}

/* ============================================================================
 * Array Serialization
 * ============================================================================ */

/*
 * Serialize an ArrowArray to ByteArray
 *
 * @extern "lean_arrow_ipc_serialize_array"
 * opaque serializeArrayRaw : ArrowArrayPtr.type → ArrowSchemaPtr.type → IO ByteArray
 */
LEAN_EXPORT lean_obj_res lean_arrow_ipc_serialize_array(
    b_lean_obj_arg array_ptr_obj,
    b_lean_obj_arg schema_ptr_obj,
    lean_obj_arg w
) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);

    if (!array) {
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Null array pointer")));
    }
    if (!schema) {
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Null schema pointer")));
    }

    uint8_t* data = NULL;
    size_t size = 0;

    int rc = arrow_ipc_serialize_array(array, schema, &data, &size);
    if (rc != ARROW_IPC_OK) {
        return lean_io_result_mk_error(make_ipc_error(rc));
    }

    /* Create Lean ByteArray */
    lean_object* byte_array = lean_alloc_sarray(1, size, size);
    memcpy(lean_sarray_cptr(byte_array), data, size);
    free(data);

    return lean_io_result_mk_ok(byte_array);
}

/*
 * Deserialize a ByteArray to ArrowArray
 *
 * @extern "lean_arrow_ipc_deserialize_array"
 * opaque deserializeArrayRaw : @& ByteArray → ArrowSchemaPtr.type → IO (Option ArrowArrayPtr.type)
 */
LEAN_EXPORT lean_obj_res lean_arrow_ipc_deserialize_array(
    b_lean_obj_arg byte_array,
    b_lean_obj_arg schema_ptr_obj,
    lean_obj_arg w
) {
    const uint8_t* data = lean_sarray_cptr(byte_array);
    size_t size = lean_sarray_size(byte_array);
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);

    struct ArrowArray* array = (struct ArrowArray*)calloc(1, sizeof(struct ArrowArray));
    if (!array) {
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Failed to allocate array")));
    }

    int rc = arrow_ipc_deserialize_array(data, size, schema, array, NULL);
    if (rc != ARROW_IPC_OK) {
        free(array);
        return lean_io_result_mk_ok(lean_mk_option_none_ipc());
    }

    lean_object* array_obj = lean_box_usize((uintptr_t)array);
    return lean_io_result_mk_ok(lean_mk_option_some_ipc(array_obj));
}

/* ============================================================================
 * RecordBatch Serialization
 * ============================================================================ */

/*
 * Serialize a RecordBatch (schema + array) to ByteArray
 *
 * @extern "lean_arrow_ipc_serialize_batch"
 * opaque serializeBatchRaw : ArrowSchemaPtr.type → ArrowArrayPtr.type → IO ByteArray
 */
LEAN_EXPORT lean_obj_res lean_arrow_ipc_serialize_batch(
    b_lean_obj_arg schema_ptr_obj,
    b_lean_obj_arg array_ptr_obj,
    lean_obj_arg w
) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);

    if (!schema) {
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Null schema pointer")));
    }
    if (!array) {
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Null array pointer")));
    }

    uint8_t* data = NULL;
    size_t size = 0;

    int rc = arrow_ipc_serialize_batch(schema, array, &data, &size);
    if (rc != ARROW_IPC_OK) {
        return lean_io_result_mk_error(make_ipc_error(rc));
    }

    /* Create Lean ByteArray */
    lean_object* byte_array = lean_alloc_sarray(1, size, size);
    memcpy(lean_sarray_cptr(byte_array), data, size);
    free(data);

    return lean_io_result_mk_ok(byte_array);
}

/*
 * Deserialize a ByteArray to RecordBatch (returns schema and array)
 *
 * @extern "lean_arrow_ipc_deserialize_batch"
 * opaque deserializeBatchRaw : @& ByteArray → IO (Option (ArrowSchemaPtr.type × ArrowArrayPtr.type))
 */
LEAN_EXPORT lean_obj_res lean_arrow_ipc_deserialize_batch(
    b_lean_obj_arg byte_array,
    lean_obj_arg w
) {
    const uint8_t* data = lean_sarray_cptr(byte_array);
    size_t size = lean_sarray_size(byte_array);

    struct ArrowSchema* schema = (struct ArrowSchema*)calloc(1, sizeof(struct ArrowSchema));
    struct ArrowArray* array = (struct ArrowArray*)calloc(1, sizeof(struct ArrowArray));

    if (!schema || !array) {
        if (schema) free(schema);
        if (array) free(array);
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Failed to allocate schema/array")));
    }

    int rc = arrow_ipc_deserialize_batch(data, size, schema, array);
    if (rc != ARROW_IPC_OK) {
        free(schema);
        free(array);
        return lean_io_result_mk_ok(lean_mk_option_none_ipc());
    }

    /* Create tuple (schema, array) as Prod */
    lean_object* schema_obj = lean_box_usize((uintptr_t)schema);
    lean_object* array_obj = lean_box_usize((uintptr_t)array);
    lean_object* pair = lean_alloc_ctor(0, 2, 0);
    lean_ctor_set(pair, 0, schema_obj);
    lean_ctor_set(pair, 1, array_obj);

    return lean_io_result_mk_ok(lean_mk_option_some_ipc(pair));
}

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/*
 * Get the serialized size of a batch
 *
 * @extern "lean_arrow_ipc_batch_size"
 * opaque batchSerializedSize : ArrowSchemaPtr.type → ArrowArrayPtr.type → IO UInt64
 */
LEAN_EXPORT lean_obj_res lean_arrow_ipc_batch_size(
    b_lean_obj_arg schema_ptr_obj,
    b_lean_obj_arg array_ptr_obj,
    lean_obj_arg w
) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);

    if (!schema || !array) {
        return lean_io_result_mk_error(
            lean_mk_io_user_error(lean_mk_string("Null pointer")));
    }

    uint8_t* data = NULL;
    size_t size = 0;

    int rc = arrow_ipc_serialize_batch(schema, array, &data, &size);
    if (rc != ARROW_IPC_OK) {
        return lean_io_result_mk_error(make_ipc_error(rc));
    }

    free(data);
    return lean_io_result_mk_ok(lean_box_uint64((uint64_t)size));
}
