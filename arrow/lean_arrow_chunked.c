/**
 * lean_arrow_chunked.c - Lean FFI wrappers for ChunkedArray and Table
 */

#include <lean/lean.h>
#include "arrow_chunked.h"
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

// External class for ChunkedArray
static lean_external_class* g_chunked_array_class = NULL;

// External class for Table
static lean_external_class* g_table_class = NULL;

static void chunked_array_finalize(void* ptr) {
    chunked_array_free((ChunkedArray*)ptr);
}

static void chunked_array_foreach(void* ptr, b_lean_obj_arg fn) {
    (void)ptr; (void)fn;
}

static void table_finalize(void* ptr) {
    table_free((Table*)ptr);
}

static void table_foreach(void* ptr, b_lean_obj_arg fn) {
    (void)ptr; (void)fn;
}

static void init_external_classes(void) {
    if (g_chunked_array_class == NULL) {
        g_chunked_array_class = lean_register_external_class(chunked_array_finalize, chunked_array_foreach);
    }
    if (g_table_class == NULL) {
        g_table_class = lean_register_external_class(table_finalize, table_foreach);
    }
}

// Helper functions for Option type
static lean_obj_res lean_mk_option_none(void) {
    return lean_alloc_ctor(0, 0, 0);
}

static lean_obj_res lean_mk_option_some(lean_obj_arg value) {
    lean_object* option = lean_alloc_ctor(1, 1, 0);
    lean_ctor_set(option, 0, value);
    return option;
}

// ============================================================================
// ChunkedArray FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_chunked_array_create(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    init_external_classes();

    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    ChunkedArray* ca = chunked_array_create(schema);
    if (!ca) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    lean_object* external = lean_alloc_external(g_chunked_array_class, ca);
    return lean_io_result_mk_ok(lean_mk_option_some(external));
}

LEAN_EXPORT lean_obj_res lean_chunked_array_from_array(b_lean_obj_arg array_ptr_obj, b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    init_external_classes();

    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);

    if (!array || !schema) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    ChunkedArray* ca = chunked_array_from_array(array, schema);
    if (!ca) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    lean_object* external = lean_alloc_external(g_chunked_array_class, ca);
    return lean_io_result_mk_ok(lean_mk_option_some(external));
}

LEAN_EXPORT lean_obj_res lean_chunked_array_add_chunk(lean_obj_arg ca_obj, b_lean_obj_arg chunk_ptr_obj, lean_obj_arg w) {
    ChunkedArray* ca = (ChunkedArray*)lean_get_external_data(ca_obj);
    struct ArrowArray* chunk = (struct ArrowArray*)lean_unbox_usize(chunk_ptr_obj);

    if (!ca || !chunk) {
        return lean_io_result_mk_ok(lean_box(0)); // false
    }

    int result = chunked_array_add_chunk(ca, chunk);
    return lean_io_result_mk_ok(lean_box(result == 0 ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_chunked_array_get_chunk(b_lean_obj_arg ca_obj, uint64_t index, lean_obj_arg w) {
    ChunkedArray* ca = (ChunkedArray*)lean_get_external_data(ca_obj);
    if (!ca) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    struct ArrowArray* chunk = chunked_array_get_chunk(ca, (size_t)index);
    if (!chunk) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    // Return pointer to the chunk (caller doesn't own it)
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)chunk)));
}

LEAN_EXPORT lean_obj_res lean_chunked_array_num_chunks(b_lean_obj_arg ca_obj, lean_obj_arg w) {
    ChunkedArray* ca = (ChunkedArray*)lean_get_external_data(ca_obj);
    size_t count = chunked_array_num_chunks(ca);
    return lean_io_result_mk_ok(lean_box_uint64((uint64_t)count));
}

LEAN_EXPORT lean_obj_res lean_chunked_array_length(b_lean_obj_arg ca_obj, lean_obj_arg w) {
    ChunkedArray* ca = (ChunkedArray*)lean_get_external_data(ca_obj);
    int64_t length = chunked_array_length(ca);
    return lean_io_result_mk_ok(lean_box_uint64((uint64_t)length));
}

LEAN_EXPORT lean_obj_res lean_chunked_array_null_count(b_lean_obj_arg ca_obj, lean_obj_arg w) {
    ChunkedArray* ca = (ChunkedArray*)lean_get_external_data(ca_obj);
    int64_t count = chunked_array_null_count(ca);
    return lean_io_result_mk_ok(lean_box_uint64((uint64_t)count));
}

LEAN_EXPORT lean_obj_res lean_chunked_array_type(b_lean_obj_arg ca_obj, lean_obj_arg w) {
    ChunkedArray* ca = (ChunkedArray*)lean_get_external_data(ca_obj);
    struct ArrowSchema* type = chunked_array_type(ca);

    if (!type) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)type)));
}

LEAN_EXPORT lean_obj_res lean_chunked_array_slice(lean_obj_arg ca_obj, int64_t offset, int64_t length, lean_obj_arg w) {
    init_external_classes();

    ChunkedArray* ca = (ChunkedArray*)lean_get_external_data(ca_obj);
    if (!ca) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    ChunkedArray* result = chunked_array_slice(ca, offset, length);
    if (!result) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    lean_object* external = lean_alloc_external(g_chunked_array_class, result);
    return lean_io_result_mk_ok(lean_mk_option_some(external));
}

// ============================================================================
// Table FFI
// ============================================================================

LEAN_EXPORT lean_obj_res lean_table_create(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    init_external_classes();

    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    Table* table = table_create(schema);
    if (!table) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    lean_object* external = lean_alloc_external(g_table_class, table);
    return lean_io_result_mk_ok(lean_mk_option_some(external));
}

LEAN_EXPORT lean_obj_res lean_table_from_record_batch(b_lean_obj_arg batch_ptr_obj, b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    init_external_classes();

    struct ArrowArray* batch = (struct ArrowArray*)lean_unbox_usize(batch_ptr_obj);
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);

    if (!batch || !schema) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    Table* table = table_from_record_batch(batch, schema);
    if (!table) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    lean_object* external = lean_alloc_external(g_table_class, table);
    return lean_io_result_mk_ok(lean_mk_option_some(external));
}

LEAN_EXPORT lean_obj_res lean_table_get_column(b_lean_obj_arg table_obj, uint64_t index, lean_obj_arg w) {
    init_external_classes();

    Table* table = (Table*)lean_get_external_data(table_obj);
    if (!table) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    ChunkedArray* column = table_get_column(table, (size_t)index);
    if (!column) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    // Return a reference to the column (not owned by caller)
    // We wrap it but don't transfer ownership
    lean_object* external = lean_alloc_external(g_chunked_array_class, column);
    return lean_io_result_mk_ok(lean_mk_option_some(external));
}

LEAN_EXPORT lean_obj_res lean_table_get_column_by_name(b_lean_obj_arg table_obj, b_lean_obj_arg name_obj, lean_obj_arg w) {
    init_external_classes();

    Table* table = (Table*)lean_get_external_data(table_obj);
    const char* name = lean_string_cstr(name_obj);

    if (!table || !name) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    ChunkedArray* column = table_get_column_by_name(table, name);
    if (!column) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    lean_object* external = lean_alloc_external(g_chunked_array_class, column);
    return lean_io_result_mk_ok(lean_mk_option_some(external));
}

LEAN_EXPORT lean_obj_res lean_table_num_columns(b_lean_obj_arg table_obj, lean_obj_arg w) {
    Table* table = (Table*)lean_get_external_data(table_obj);
    size_t count = table_num_columns(table);
    return lean_io_result_mk_ok(lean_box_uint64((uint64_t)count));
}

LEAN_EXPORT lean_obj_res lean_table_num_rows(b_lean_obj_arg table_obj, lean_obj_arg w) {
    Table* table = (Table*)lean_get_external_data(table_obj);
    int64_t count = table_num_rows(table);
    return lean_io_result_mk_ok(lean_box_uint64((uint64_t)count));
}

LEAN_EXPORT lean_obj_res lean_table_schema(b_lean_obj_arg table_obj, lean_obj_arg w) {
    Table* table = (Table*)lean_get_external_data(table_obj);
    struct ArrowSchema* schema = table_schema(table);

    if (!schema) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)schema)));
}

LEAN_EXPORT lean_obj_res lean_table_column_name(b_lean_obj_arg table_obj, uint64_t index, lean_obj_arg w) {
    Table* table = (Table*)lean_get_external_data(table_obj);
    const char* name = table_column_name(table, (size_t)index);

    if (!name) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    return lean_io_result_mk_ok(lean_mk_option_some(lean_mk_string(name)));
}

LEAN_EXPORT lean_obj_res lean_table_slice(lean_obj_arg table_obj, int64_t offset, int64_t length, lean_obj_arg w) {
    init_external_classes();

    Table* table = (Table*)lean_get_external_data(table_obj);
    if (!table) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    Table* result = table_slice(table, offset, length);
    if (!result) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    lean_object* external = lean_alloc_external(g_table_class, result);
    return lean_io_result_mk_ok(lean_mk_option_some(external));
}

#ifdef __cplusplus
}
#endif
