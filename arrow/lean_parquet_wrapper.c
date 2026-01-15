#include <lean/lean.h>
#include "parquet_wrapper.h"
#include "arrow_wrapper.h"
#include <string.h>
#include <stdlib.h>

// Lean 4 FFI wrapper functions for Parquet operations

// Helper functions for Option type
static lean_obj_res lean_mk_option_none(void) {
    return lean_alloc_ctor(0, 0, 0);
}

static lean_obj_res lean_mk_option_some(lean_obj_arg value) {
    lean_object * option = lean_alloc_ctor(1, 1, 0);
    lean_ctor_set(option, 0, value);
    return option;
}

// Convert Lean string array to C string array
static char** lean_array_to_c_strings(lean_object* lean_array, size_t* out_count) {
    if (!lean_array || !out_count) return NULL;
    
    size_t count = lean_array_size(lean_array);
    *out_count = count;
    
    if (count == 0) return NULL;
    
    char** c_strings = malloc(count * sizeof(char*));
    if (!c_strings) return NULL;
    
    for (size_t i = 0; i < count; i++) {
        lean_object* lean_str = lean_array_get_core(lean_array, i);
        const char* str = lean_string_cstr(lean_str);
        c_strings[i] = malloc(strlen(str) + 1);
        if (c_strings[i]) {
            strcpy(c_strings[i], str);
        }
    }
    
    return c_strings;
}

static void free_c_strings(char** strings, size_t count) {
    if (!strings) return;
    for (size_t i = 0; i < count; i++) {
        if (strings[i]) {
            free(strings[i]);
        }
    }
    free(strings);
}

// Lean Parquet Reader functions
LEAN_EXPORT lean_obj_res lean_parquet_reader_open(b_lean_obj_arg file_path, lean_obj_arg w) {
    const char* path = lean_string_cstr(file_path);
    struct ParquetReader* reader = parquet_reader_open(path);
    
    if (reader) {
        lean_obj_res ptr = lean_box_usize((uintptr_t)reader);
        return lean_io_result_mk_ok(lean_mk_option_some(ptr));
    } else {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
}

LEAN_EXPORT lean_obj_res lean_parquet_reader_close(b_lean_obj_arg reader_ptr, lean_obj_arg w) {
    struct ParquetReader* reader = (struct ParquetReader*)lean_unbox_usize(reader_ptr);
    if (reader) {
        parquet_reader_close(reader);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_parquet_reader_get_metadata(b_lean_obj_arg reader_ptr, lean_obj_arg w) {
    struct ParquetReader* reader = (struct ParquetReader*)lean_unbox_usize(reader_ptr);
    if (!reader) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    
    struct ParquetFileMetadata* metadata = parquet_reader_get_metadata(reader);
    if (metadata) {
        lean_obj_res ptr = lean_box_usize((uintptr_t)metadata);
        return lean_io_result_mk_ok(lean_mk_option_some(ptr));
    } else {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
}

LEAN_EXPORT lean_obj_res lean_parquet_reader_read_table(b_lean_obj_arg reader_ptr, lean_obj_arg w) {
    struct ParquetReader* reader = (struct ParquetReader*)lean_unbox_usize(reader_ptr);
    if (!reader) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    
    struct ArrowArrayStream* stream = parquet_reader_read_table(reader);
    if (stream) {
        lean_obj_res ptr = lean_box_usize((uintptr_t)stream);
        return lean_io_result_mk_ok(lean_mk_option_some(ptr));
    } else {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
}

LEAN_EXPORT lean_obj_res lean_parquet_reader_read_row_group(b_lean_obj_arg reader_ptr, uint32_t row_group_idx, lean_obj_arg w) {
    struct ParquetReader* reader = (struct ParquetReader*)lean_unbox_usize(reader_ptr);
    if (!reader) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    
    struct ArrowArrayStream* stream = parquet_reader_read_row_group(reader, row_group_idx);
    if (stream) {
        lean_obj_res ptr = lean_box_usize((uintptr_t)stream);
        return lean_io_result_mk_ok(lean_mk_option_some(ptr));
    } else {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
}

LEAN_EXPORT lean_obj_res lean_parquet_reader_read_columns(b_lean_obj_arg reader_ptr, b_lean_obj_arg columns_array, lean_obj_arg w) {
    struct ParquetReader* reader = (struct ParquetReader*)lean_unbox_usize(reader_ptr);
    if (!reader) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    
    size_t num_columns;
    char** columns = lean_array_to_c_strings(columns_array, &num_columns);
    
    struct ArrowArrayStream* stream = parquet_reader_read_columns(reader, (const char**)columns, num_columns);
    
    free_c_strings(columns, num_columns);
    
    if (stream) {
        lean_obj_res ptr = lean_box_usize((uintptr_t)stream);
        return lean_io_result_mk_ok(lean_mk_option_some(ptr));
    } else {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
}

// Lean Parquet Writer functions
LEAN_EXPORT lean_obj_res lean_parquet_writer_open(b_lean_obj_arg file_path, b_lean_obj_arg schema_ptr, lean_obj_arg w) {
    const char* path = lean_string_cstr(file_path);
    struct ArrowSchema* schema = (struct ArrowSchema*)lean_unbox_usize(schema_ptr);
    
    if (!schema) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    
    struct ParquetWriter* writer = parquet_writer_open(path, schema);
    if (writer) {
        lean_obj_res ptr = lean_box_usize((uintptr_t)writer);
        return lean_io_result_mk_ok(lean_mk_option_some(ptr));
    } else {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
}

LEAN_EXPORT lean_obj_res lean_parquet_writer_close(b_lean_obj_arg writer_ptr, lean_obj_arg w) {
    struct ParquetWriter* writer = (struct ParquetWriter*)lean_unbox_usize(writer_ptr);
    if (writer) {
        parquet_writer_close(writer);
    }
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_parquet_writer_write_table(b_lean_obj_arg writer_ptr, b_lean_obj_arg stream_ptr, lean_obj_arg w) {
    struct ParquetWriter* writer = (struct ParquetWriter*)lean_unbox_usize(writer_ptr);
    struct ArrowArrayStream* stream = (struct ArrowArrayStream*)lean_unbox_usize(stream_ptr);
    
    if (!writer || !stream) {
        return lean_io_result_mk_ok(lean_box(0)); /* false */
    }
    
    bool success = parquet_writer_write_table(writer, stream);
    return lean_io_result_mk_ok(lean_box(success ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_parquet_writer_write_batch(b_lean_obj_arg writer_ptr, b_lean_obj_arg array_ptr, lean_obj_arg w) {
    struct ParquetWriter* writer = (struct ParquetWriter*)lean_unbox_usize(writer_ptr);
    struct ArrowArray* array = (struct ArrowArray*)lean_unbox_usize(array_ptr);
    
    if (!writer || !array) {
        return lean_io_result_mk_ok(lean_box(0)); /* false */
    }
    
    bool success = parquet_writer_write_batch(writer, array);
    return lean_io_result_mk_ok(lean_box(success ? 1 : 0));
}

LEAN_EXPORT lean_obj_res lean_parquet_writer_set_compression(b_lean_obj_arg writer_ptr, uint32_t compression, lean_obj_arg w) {
    struct ParquetWriter* writer = (struct ParquetWriter*)lean_unbox_usize(writer_ptr);
    if (!writer) {
        return lean_io_result_mk_ok(lean_box(0));
    }
    
    parquet_writer_set_compression(writer, compression);
    return lean_io_result_mk_ok(lean_box(0));
}

// Lean Parquet Metadata functions
LEAN_EXPORT lean_obj_res lean_parquet_metadata_get_num_rows(b_lean_obj_arg metadata_ptr, lean_obj_arg w) {
    struct ParquetFileMetadata* metadata = (struct ParquetFileMetadata*)lean_unbox_usize(metadata_ptr);
    if (!metadata) {
        return lean_io_result_mk_ok(lean_box_uint64(0));
    }
    
    uint64_t num_rows = parquet_metadata_get_num_rows(metadata);
    return lean_io_result_mk_ok(lean_box_uint64(num_rows));
}

LEAN_EXPORT lean_obj_res lean_parquet_metadata_get_num_row_groups(b_lean_obj_arg metadata_ptr, lean_obj_arg w) {
    struct ParquetFileMetadata* metadata = (struct ParquetFileMetadata*)lean_unbox_usize(metadata_ptr);
    if (!metadata) {
        return lean_io_result_mk_ok(lean_box_uint32(0));
    }
    
    uint32_t num_row_groups = parquet_metadata_get_num_row_groups(metadata);
    return lean_io_result_mk_ok(lean_box_uint32(num_row_groups));
}

LEAN_EXPORT lean_obj_res lean_parquet_metadata_get_file_size(b_lean_obj_arg metadata_ptr, lean_obj_arg w) {
    struct ParquetFileMetadata* metadata = (struct ParquetFileMetadata*)lean_unbox_usize(metadata_ptr);
    if (!metadata) {
        return lean_io_result_mk_ok(lean_box_uint64(0));
    }
    
    uint64_t file_size = parquet_metadata_get_file_size(metadata);
    return lean_io_result_mk_ok(lean_box_uint64(file_size));
}

LEAN_EXPORT lean_obj_res lean_parquet_metadata_get_row_group(b_lean_obj_arg metadata_ptr, uint32_t index, lean_obj_arg w) {
    struct ParquetFileMetadata* metadata = (struct ParquetFileMetadata*)lean_unbox_usize(metadata_ptr);
    if (!metadata) {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
    
    struct ParquetRowGroupMetadata* row_group = parquet_metadata_get_row_group(metadata, index);
    if (row_group) {
        lean_obj_res ptr = lean_box_usize((uintptr_t)row_group);
        return lean_io_result_mk_ok(lean_mk_option_some(ptr));
    } else {
        return lean_io_result_mk_ok(lean_mk_option_none());
    }
}

// Lean Parquet Row Group Metadata functions
LEAN_EXPORT lean_obj_res lean_parquet_row_group_get_num_rows(b_lean_obj_arg row_group_ptr, lean_obj_arg w) {
    struct ParquetRowGroupMetadata* row_group = (struct ParquetRowGroupMetadata*)lean_unbox_usize(row_group_ptr);
    if (!row_group) {
        return lean_io_result_mk_ok(lean_box_uint64(0));
    }
    
    uint64_t num_rows = parquet_row_group_get_num_rows(row_group);
    return lean_io_result_mk_ok(lean_box_uint64(num_rows));
}

LEAN_EXPORT lean_obj_res lean_parquet_row_group_get_num_columns(b_lean_obj_arg row_group_ptr, lean_obj_arg w) {
    struct ParquetRowGroupMetadata* row_group = (struct ParquetRowGroupMetadata*)lean_unbox_usize(row_group_ptr);
    if (!row_group) {
        return lean_io_result_mk_ok(lean_box_uint32(0));
    }
    
    uint32_t num_columns = parquet_row_group_get_num_columns(row_group);
    return lean_io_result_mk_ok(lean_box_uint32(num_columns));
}

LEAN_EXPORT lean_obj_res lean_parquet_row_group_get_total_byte_size(b_lean_obj_arg row_group_ptr, lean_obj_arg w) {
    struct ParquetRowGroupMetadata* row_group = (struct ParquetRowGroupMetadata*)lean_unbox_usize(row_group_ptr);
    if (!row_group) {
        return lean_io_result_mk_ok(lean_box_uint64(0));
    }
    
    uint64_t total_byte_size = parquet_row_group_get_total_byte_size(row_group);
    return lean_io_result_mk_ok(lean_box_uint64(total_byte_size));
}
