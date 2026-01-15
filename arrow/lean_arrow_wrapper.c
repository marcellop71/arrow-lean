#define _GNU_SOURCE
#include "arrow_wrapper.h"
#include <lean/lean.h>
#include <stdlib.h>
#include <string.h>

// Lean 4 FFI wrapper functions for Arrow C interface

// External class for Arrow buffers
static lean_external_class * g_arrow_buffer_external_class = NULL;

static void arrow_buffer_finalize(void * ptr) {
    // Arrow buffer cleanup if needed
    (void)ptr; // Unused for now
}

static void arrow_buffer_foreach(void * ptr, b_lean_obj_arg fn) {
    // No internal Lean objects to traverse
    (void)ptr; (void)fn;
}

// Initialize external class (should be called once)
static void init_external_classes(void) {
    if (g_arrow_buffer_external_class == NULL) {
        g_arrow_buffer_external_class = lean_register_external_class(arrow_buffer_finalize, arrow_buffer_foreach);
    }
}

// Helper functions for Option type
static lean_obj_res lean_mk_option_none(void) {
    return lean_alloc_ctor(0, 0, 0);
}

static lean_obj_res lean_mk_option_some(lean_obj_arg value) {
    lean_object * option = lean_alloc_ctor(1, 1, 0);
    lean_ctor_set(option, 0, value);
    return option;
}

static lean_obj_res lean_mk_external_buffer(void * data) {
    init_external_classes();
    return lean_alloc_external(g_arrow_buffer_external_class, data);
}

// Schema operations with Lean FFI
LEAN_EXPORT lean_obj_res lean_arrow_schema_init(b_lean_obj_arg format_obj, lean_obj_arg w) {
    const char* format = lean_string_cstr(format_obj);
    struct ArrowSchema* schema = arrow_schema_init(format);
    
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Failed to initialize schema"));
    }
    
    return lean_io_result_mk_ok(lean_box_usize((uintptr_t)schema));
}

LEAN_EXPORT lean_obj_res lean_arrow_schema_add_child(b_lean_obj_arg schema_ptr_obj, b_lean_obj_arg child_ptr_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    struct ArrowSchema* child = (struct ArrowSchema*)lean_unbox_usize(child_ptr_obj);
    
    int result = arrow_schema_add_child(schema, child);
    if (result != 0) {
        return lean_io_result_mk_error(lean_mk_string("Failed to add child to schema"));
    }
    
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_arrow_schema_release(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    arrow_schema_release(schema);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_arrow_schema_get_format(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema || !schema->format) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema or missing format"));
    }
    
    lean_obj_res format_str = lean_mk_string(schema->format);
    return lean_io_result_mk_ok(format_str);
}

LEAN_EXPORT lean_obj_res lean_arrow_schema_get_name(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema"));
    }
    
    if (!schema->name) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    
    lean_obj_res name_str = lean_mk_string(schema->name);
    return lean_io_result_mk_ok(lean_mk_option_some(name_str));
}

LEAN_EXPORT lean_obj_res lean_arrow_schema_get_flags(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema"));
    }

    return lean_io_result_mk_ok(lean_box_uint64(schema->flags));
}

// Schema introspection: get number of children
LEAN_EXPORT lean_obj_res lean_arrow_schema_get_child_count(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema"));
    }

    return lean_io_result_mk_ok(lean_box_uint64((uint64_t)schema->n_children));
}

// Schema introspection: get child schema by index
LEAN_EXPORT lean_obj_res lean_arrow_schema_get_child(b_lean_obj_arg schema_ptr_obj, uint64_t index, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema"));
    }

    if ((int64_t)index >= schema->n_children || !schema->children) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    struct ArrowSchema* child = schema->children[index];
    if (!child) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    // Return the child schema pointer (caller should not release it, it's owned by parent)
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)child)));
}

// Schema introspection: check if nullable
LEAN_EXPORT lean_obj_res lean_arrow_schema_is_nullable(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema"));
    }

    // ARROW_FLAG_NULLABLE = 2
    bool is_nullable = (schema->flags & 2) != 0;
    return lean_io_result_mk_ok(lean_box(is_nullable ? 1 : 0));
}

// Schema introspection: check if dictionary-ordered
LEAN_EXPORT lean_obj_res lean_arrow_schema_is_dictionary_ordered(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema"));
    }

    // ARROW_FLAG_DICTIONARY_ORDERED = 1
    bool is_ordered = (schema->flags & 1) != 0;
    return lean_io_result_mk_ok(lean_box(is_ordered ? 1 : 0));
}

// Schema introspection: check if map keys are sorted
LEAN_EXPORT lean_obj_res lean_arrow_schema_is_map_keys_sorted(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema"));
    }

    // ARROW_FLAG_MAP_KEYS_SORTED = 4
    bool is_sorted = (schema->flags & 4) != 0;
    return lean_io_result_mk_ok(lean_box(is_sorted ? 1 : 0));
}

// Schema introspection: get dictionary schema (if any)
LEAN_EXPORT lean_obj_res lean_arrow_schema_get_dictionary(b_lean_obj_arg schema_ptr_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema"));
    }

    if (!schema->dictionary) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)schema->dictionary)));
}

// Schema: set name
LEAN_EXPORT lean_obj_res lean_arrow_schema_set_name(b_lean_obj_arg schema_ptr_obj, b_lean_obj_arg name_obj, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema"));
    }

    const char* name = lean_string_cstr(name_obj);

    // Access private data to store the name
    struct ArrowSchemaPrivate {
        char* format_copy;
        char* name_copy;
        char* metadata_copy;
        struct ArrowSchema** children_storage;
    };

    struct ArrowSchemaPrivate* private_data = (struct ArrowSchemaPrivate*)schema->private_data;
    if (private_data) {
        free(private_data->name_copy);
        private_data->name_copy = strdup(name);
        schema->name = private_data->name_copy;
    }

    return lean_io_result_mk_ok(lean_box(0));
}

// Schema: set flags
LEAN_EXPORT lean_obj_res lean_arrow_schema_set_flags(b_lean_obj_arg schema_ptr_obj, uint64_t flags, lean_obj_arg w) {
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr_obj);
    if (!schema) {
        return lean_io_result_mk_error(lean_mk_string("Invalid schema"));
    }

    schema->flags = flags;
    return lean_io_result_mk_ok(lean_box(0));
}

// Array introspection: get number of children
LEAN_EXPORT lean_obj_res lean_arrow_array_get_child_count(b_lean_obj_arg array_ptr_obj, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    if (!array) {
        return lean_io_result_mk_error(lean_mk_string("Invalid array"));
    }

    return lean_io_result_mk_ok(lean_box_uint64((uint64_t)array->n_children));
}

// Array introspection: get child array by index
LEAN_EXPORT lean_obj_res lean_arrow_array_get_child(b_lean_obj_arg array_ptr_obj, uint64_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    if (!array) {
        return lean_io_result_mk_error(lean_mk_string("Invalid array"));
    }

    if ((int64_t)index >= array->n_children || !array->children) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    struct ArrowArray* child = array->children[index];
    if (!child) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    // Return the child array pointer (caller should not release it, it's owned by parent)
    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)child)));
}

// Array introspection: get number of buffers
LEAN_EXPORT lean_obj_res lean_arrow_array_get_buffer_count(b_lean_obj_arg array_ptr_obj, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    if (!array) {
        return lean_io_result_mk_error(lean_mk_string("Invalid array"));
    }

    return lean_io_result_mk_ok(lean_box_uint64((uint64_t)array->n_buffers));
}

// Array introspection: get dictionary array (if any)
LEAN_EXPORT lean_obj_res lean_arrow_array_get_dictionary(b_lean_obj_arg array_ptr_obj, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    if (!array) {
        return lean_io_result_mk_error(lean_mk_string("Invalid array"));
    }

    if (!array->dictionary) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }

    return lean_io_result_mk_ok(lean_mk_option_some(lean_box_usize((uintptr_t)array->dictionary)));
}

// Array operations with Lean FFI
LEAN_EXPORT lean_obj_res lean_arrow_array_init(uint64_t length, lean_obj_arg w) {
    struct ArrowArray* array = arrow_array_init(length);
    if (!array) {
        return lean_io_result_mk_error(lean_mk_string("Failed to initialize array"));
    }
    
    return lean_io_result_mk_ok(lean_box_usize((uintptr_t)array));
}

LEAN_EXPORT lean_obj_res lean_arrow_array_set_buffer(b_lean_obj_arg array_ptr_obj, size_t index, b_lean_obj_arg buffer_ptr_obj, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    uint8_t* buffer = (uint8_t*)lean_get_external_data(buffer_ptr_obj);
    
    int result = arrow_array_set_buffer(array, index, buffer);
    if (result != 0) {
        return lean_io_result_mk_error(lean_mk_string("Failed to set buffer"));
    }
    
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_arrow_array_get_buffer(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    uint8_t* buffer = arrow_array_get_buffer(array, index);
    
    if (!buffer) {
        return lean_io_result_mk_error(lean_mk_string("Failed to get buffer or buffer is null"));
    }
    
    return lean_io_result_mk_ok(lean_mk_external_buffer(buffer));
}

LEAN_EXPORT lean_obj_res lean_arrow_array_release(b_lean_obj_arg array_ptr_obj, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    arrow_array_release(array);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_arrow_array_get_length(b_lean_obj_arg array_ptr_obj, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    if (!array) {
        return lean_io_result_mk_error(lean_mk_string("Invalid array"));
    }
    
    return lean_io_result_mk_ok(lean_box_uint64(array->length));
}

LEAN_EXPORT lean_obj_res lean_arrow_array_get_null_count(b_lean_obj_arg array_ptr_obj, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    if (!array) {
        return lean_io_result_mk_error(lean_mk_string("Invalid array"));
    }
    
    return lean_io_result_mk_ok(lean_box_uint64(array->null_count));
}

LEAN_EXPORT lean_obj_res lean_arrow_array_get_offset(b_lean_obj_arg array_ptr_obj, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    if (!array) {
        return lean_io_result_mk_error(lean_mk_string("Invalid array"));
    }
    
    return lean_io_result_mk_ok(lean_box_uint64(array->offset));
}

// Stream operations with Lean FFI
LEAN_EXPORT lean_obj_res lean_arrow_stream_init(lean_obj_arg w) {
    struct ArrowArrayStream* stream = arrow_stream_init();
    if (!stream) {
        return lean_io_result_mk_error(lean_mk_string("Failed to initialize stream"));
    }
    
    return lean_io_result_mk_ok(lean_box_usize((uintptr_t)stream));
}

LEAN_EXPORT lean_obj_res lean_arrow_stream_get_schema(b_lean_obj_arg stream_ptr_obj, lean_obj_arg w) {
    struct ArrowArrayStream* stream = (struct ArrowArrayStream*)lean_unbox_usize(stream_ptr_obj);
    struct ArrowSchema* schema = arrow_stream_get_schema(stream);
    
    if (!schema) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    
    lean_obj_res schema_ptr = lean_box_usize((uintptr_t)schema);
    return lean_io_result_mk_ok(lean_mk_option_some(schema_ptr));
}

LEAN_EXPORT lean_obj_res lean_arrow_stream_get_next(b_lean_obj_arg stream_ptr_obj, lean_obj_arg w) {
    struct ArrowArrayStream* stream = (struct ArrowArrayStream*)lean_unbox_usize(stream_ptr_obj);
    struct ArrowArray* array = arrow_stream_get_next(stream);
    
    if (!array) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    
    lean_obj_res array_ptr = lean_box_usize((uintptr_t)array);
    return lean_io_result_mk_ok(lean_mk_option_some(array_ptr));
}

// Data access functions with Lean FFI
LEAN_EXPORT lean_obj_res lean_arrow_get_bool_value(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    bool value = arrow_get_bool_value(array, index);
    return lean_io_result_mk_ok(lean_box(value ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_arrow_is_bool_null(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    int result = arrow_is_bool_null(array, index);
    return lean_io_result_mk_ok(lean_box(result == 1 ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_arrow_get_int64_value(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    int64_t value = arrow_get_int64_value(array, index);
    return lean_io_result_mk_ok(lean_box_uint64(value));
}

LEAN_EXPORT lean_obj_res lean_arrow_is_int64_null(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    int result = arrow_is_int64_null(array, index);
    return lean_io_result_mk_ok(lean_box(result == 1 ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_arrow_get_string_value(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    const char* value = arrow_get_string_value(array, index);
    
    if (!value) {
        return lean_io_result_mk_error(lean_mk_string("Failed to get string value"));
    }
    
    lean_obj_res str = lean_mk_string(value);
    free((char*)value); // Free the C string allocated by arrow_get_string_value
    return lean_io_result_mk_ok(str);
}

LEAN_EXPORT lean_obj_res lean_arrow_is_string_null(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    int result = arrow_is_string_null(array, index);
    return lean_io_result_mk_ok(lean_box(result == 1 ? 1 : 0));
}

// Float64 data access
LEAN_EXPORT lean_obj_res lean_arrow_get_float64_value(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    double value = arrow_get_float64_value(array, index);
    return lean_io_result_mk_ok(lean_box_float(value));
}

LEAN_EXPORT lean_obj_res lean_arrow_is_float64_null(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    int result = arrow_is_float64_null(array, index);
    return lean_io_result_mk_ok(lean_box(result == 1 ? 1 : 0));
}

// UInt64 data access
LEAN_EXPORT lean_obj_res lean_arrow_get_uint64_value(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    uint64_t value = arrow_get_uint64_value(array, index);
    return lean_io_result_mk_ok(lean_box_uint64(value));
}

LEAN_EXPORT lean_obj_res lean_arrow_is_uint64_null(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    int result = arrow_is_uint64_null(array, index);
    return lean_io_result_mk_ok(lean_box(result == 1 ? 1 : 0));
}

// Float32 data access
LEAN_EXPORT lean_obj_res lean_arrow_get_float32_value(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    float value = arrow_get_float32_value(array, index);
    return lean_io_result_mk_ok(lean_box_float((double)value));
}

LEAN_EXPORT lean_obj_res lean_arrow_is_float32_null(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    int result = arrow_is_float32_null(array, index);
    return lean_io_result_mk_ok(lean_box(result == 1 ? 1 : 0));
}

// Int32 data access
LEAN_EXPORT lean_obj_res lean_arrow_get_int32_value(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    int32_t value = arrow_get_int32_value(array, index);
    return lean_io_result_mk_ok(lean_box_uint32((uint32_t)value));
}

LEAN_EXPORT lean_obj_res lean_arrow_is_int32_null(b_lean_obj_arg array_ptr_obj, size_t index, lean_obj_arg w) {
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr_obj);
    int result = arrow_is_int32_null(array, index);
    return lean_io_result_mk_ok(lean_box(result == 1 ? 1 : 0));
}

// Buffer management with Lean FFI
LEAN_EXPORT lean_obj_res lean_arrow_allocate_buffer(size_t size, lean_obj_arg w) {
    struct ArrowBuffer* buffer = arrow_allocate_buffer(size);
    if (!buffer) {
        return lean_io_result_mk_error(lean_mk_string("Failed to allocate buffer"));
    }
    
    return lean_io_result_mk_ok(lean_box_usize((uintptr_t)buffer));
}

LEAN_EXPORT lean_obj_res lean_arrow_buffer_resize(b_lean_obj_arg buffer_ptr_obj, size_t new_size, lean_obj_arg w) {
    struct ArrowBuffer* buffer = (struct ArrowBuffer*)lean_unbox_usize(buffer_ptr_obj);
    int result = arrow_buffer_resize(buffer, new_size);
    
    if (result != 0) {
        return lean_io_result_mk_error(lean_mk_string("Failed to resize buffer"));
    }
    
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_arrow_buffer_free(b_lean_obj_arg buffer_ptr_obj, lean_obj_arg w) {
    struct ArrowBuffer* buffer = (struct ArrowBuffer*)lean_unbox_usize(buffer_ptr_obj);
    arrow_buffer_free(buffer);
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_arrow_buffer_get_size(b_lean_obj_arg buffer_ptr_obj, lean_obj_arg w) {
    struct ArrowBuffer* buffer = (struct ArrowBuffer*)lean_unbox_usize(buffer_ptr_obj);
    if (!buffer) {
        return lean_io_result_mk_error(lean_mk_string("Invalid buffer"));
    }
    
    size_t size = arrow_buffer_get_size(buffer);
    return lean_io_result_mk_ok(lean_box_usize(size));
}

LEAN_EXPORT lean_obj_res lean_arrow_buffer_get_capacity(b_lean_obj_arg buffer_ptr_obj, lean_obj_arg w) {
    struct ArrowBuffer* buffer = (struct ArrowBuffer*)lean_unbox_usize(buffer_ptr_obj);
    if (!buffer) {
        return lean_io_result_mk_error(lean_mk_string("Invalid buffer"));
    }
    
    size_t capacity = arrow_buffer_get_capacity(buffer);
    return lean_io_result_mk_ok(lean_box_usize(capacity));
}