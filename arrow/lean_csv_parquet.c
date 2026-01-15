// Lean FFI bindings for CSV to Parquet conversion
// This calls the C wrapper functions from csv_parquet_wrapper.h

#include <lean/lean.h>
#include <stdlib.h>
#include <string.h>
#include "csv_parquet_wrapper.h"

// Helper to create Lean Except.error
static lean_obj_res make_except_error(const char* msg) {
    lean_object* except = lean_alloc_ctor(0, 1, 0);  // Except.error
    lean_ctor_set(except, 0, lean_mk_string(msg));
    return lean_io_result_mk_ok(except);
}

// Helper to create Lean Except.ok Unit
static lean_obj_res make_except_ok(void) {
    lean_object* except = lean_alloc_ctor(1, 1, 0);  // Except.ok
    lean_ctor_set(except, 0, lean_box(0));  // Unit
    return lean_io_result_mk_ok(except);
}

LEAN_EXPORT lean_obj_res lean_csv_to_parquet(
    b_lean_obj_arg csv_obj,
    b_lean_obj_arg path_obj,
    b_lean_obj_arg compression_obj,
    lean_obj_arg w
) {
    const char* csv = lean_string_cstr(csv_obj);
    const char* path = lean_string_cstr(path_obj);
    const char* compression = lean_string_cstr(compression_obj);

    char* error_msg = NULL;
    int result = csv_to_parquet_c(csv, path, compression, &error_msg);

    if (result != 0) {
        lean_obj_res ret = make_except_error(error_msg ? error_msg : "Unknown error");
        if (error_msg) free(error_msg);
        return ret;
    }

    return make_except_ok();
}

LEAN_EXPORT lean_obj_res lean_merge_csv_to_parquet(
    b_lean_obj_arg csv_array_obj,
    b_lean_obj_arg path_obj,
    b_lean_obj_arg compression_obj,
    lean_obj_arg w
) {
    const char* path = lean_string_cstr(path_obj);
    const char* compression = lean_string_cstr(compression_obj);

    size_t count = lean_array_size(csv_array_obj);
    if (count == 0) {
        return make_except_ok();
    }

    // Build array of C strings
    const char** csv_strings = (const char**)malloc(count * sizeof(const char*));
    if (!csv_strings) {
        return make_except_error("Memory allocation failed");
    }

    for (size_t i = 0; i < count; i++) {
        lean_object* str_obj = lean_array_get_core(csv_array_obj, i);
        csv_strings[i] = lean_string_cstr(str_obj);
    }

    char* error_msg = NULL;
    int result = merge_csv_to_parquet_c(csv_strings, (int)count, path, compression, &error_msg);

    free(csv_strings);

    if (result != 0) {
        lean_obj_res ret = make_except_error(error_msg ? error_msg : "Unknown error");
        if (error_msg) free(error_msg);
        return ret;
    }

    return make_except_ok();
}

LEAN_EXPORT lean_obj_res lean_arrow_parquet_available(lean_obj_arg w) {
    int available = arrow_parquet_available_c();
    return lean_io_result_mk_ok(lean_box(available ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_arrow_version(lean_obj_arg w) {
    const char* version = arrow_version_c();
    return lean_io_result_mk_ok(lean_mk_string(version));
}
