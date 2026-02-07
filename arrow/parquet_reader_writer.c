#include "parquet_wrapper.h"
#include "parquet_writer_impl.h"
#include "parquet_reader_impl.h"
#include "arrow_wrapper.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

// Internal Parquet structure definitions
struct ParquetReader {
    char* file_path;
    bool is_open;
    ParquetFileReader* impl;  // Use the new pure C implementation
};

struct ParquetWriter {
    ParquetFileWriter* impl;  // Use the new pure C implementation
    char* file_path;
    bool is_open;
    uint32_t compression;
};

struct ParquetFileMetadata {
    void* cpp_metadata;  // Will hold actual parquet::FileMetaData*
    uint64_t num_rows;
    uint32_t num_row_groups;
    uint64_t file_size;
};

struct ParquetRowGroupMetadata {
    void* cpp_metadata;  // Will hold actual parquet::RowGroupMetaData*
    uint64_t num_rows;
    uint32_t num_columns;
    uint64_t total_byte_size;
};

struct ParquetColumnMetadata {
    void* cpp_metadata;  // Will hold actual parquet::ColumnChunkMetaData*
    char* column_name;
    uint32_t compression;
    uint32_t encoding;
};

// Global state
static bool parquet_initialized = false;

// Initialize Parquet library
static bool ensure_parquet_initialized(void) {
    if (!parquet_initialized) {
        // In a real implementation, this would initialize the Parquet C++ library
        // For now, we'll just mark it as initialized
        parquet_initialized = true;
    }
    return parquet_initialized;
}

// Parquet Reader operations
struct ParquetReader* parquet_reader_open(const char* file_path) {
    if (!ensure_parquet_initialized() || !file_path) {
        return NULL;
    }

    struct ParquetReader* reader = malloc(sizeof(struct ParquetReader));
    if (!reader) return NULL;

    reader->file_path = malloc(strlen(file_path) + 1);
    if (!reader->file_path) {
        free(reader);
        return NULL;
    }
    strcpy(reader->file_path, file_path);

    // Open using the new pure C implementation
    reader->impl = parquet_file_reader_open(file_path);
    if (!reader->impl) {
        free(reader->file_path);
        free(reader);
        return NULL;
    }

    reader->is_open = true;
    return reader;
}

void parquet_reader_close(struct ParquetReader* reader) {
    if (!reader) return;

    if (reader->impl) {
        parquet_file_reader_close(reader->impl);
        reader->impl = NULL;
    }

    if (reader->file_path) {
        free(reader->file_path);
    }

    reader->is_open = false;
    free(reader);
}

struct ParquetFileMetadata* parquet_reader_get_metadata(struct ParquetReader* reader) {
    if (!reader || !reader->is_open || !reader->impl) return NULL;

    const ParquetFileMeta* meta = parquet_file_reader_get_metadata(reader->impl);
    if (!meta) return NULL;

    struct ParquetFileMetadata* metadata = malloc(sizeof(struct ParquetFileMetadata));
    if (!metadata) return NULL;

    metadata->cpp_metadata = (void*)meta;  // Store reference to internal metadata
    metadata->num_rows = (uint64_t)meta->num_rows;
    metadata->num_row_groups = (uint32_t)meta->num_row_groups;
    metadata->file_size = 0;  // Not stored in internal metadata

    return metadata;
}

struct ArrowArrayStream* parquet_reader_read_table(struct ParquetReader* reader) {
    if (!reader || !reader->is_open || !reader->impl) return NULL;

    // Read all row groups using the pure C implementation
    return parquet_file_reader_read_all(reader->impl);
}

struct ArrowArrayStream* parquet_reader_read_row_group(struct ParquetReader* reader, uint32_t row_group) {
    if (!reader || !reader->is_open || !reader->impl) return NULL;

    // Read specific row group using the pure C implementation
    return parquet_file_reader_read_row_group(reader->impl, (int)row_group);
}

struct ArrowArrayStream* parquet_reader_read_columns(struct ParquetReader* reader, const char** columns, size_t num_columns) {
    if (!reader || !reader->is_open || !reader->impl || !columns) return NULL;

    // For now, read all columns (column projection not yet implemented)
    // TODO: Map column names to indices and use parquet_file_reader_read_columns
    return parquet_file_reader_read_all(reader->impl);
}

// Parquet Writer operations
struct ParquetWriter* parquet_writer_open(const char* file_path, struct ArrowSchema* schema) {
    if (!ensure_parquet_initialized() || !file_path || !schema) {
        return NULL;
    }

    struct ParquetWriter* writer = malloc(sizeof(struct ParquetWriter));
    if (!writer) return NULL;

    writer->file_path = malloc(strlen(file_path) + 1);
    if (!writer->file_path) {
        free(writer);
        return NULL;
    }
    strcpy(writer->file_path, file_path);

    // Create the pure C Parquet writer
    writer->impl = parquet_file_writer_create(file_path);
    if (!writer->impl) {
        free(writer->file_path);
        free(writer);
        return NULL;
    }

    // Set schema from Arrow schema
    if (parquet_file_writer_set_schema_from_arrow(writer->impl, schema) != 0) {
        parquet_file_writer_free(writer->impl);
        free(writer->file_path);
        free(writer);
        return NULL;
    }

    writer->is_open = true;
    writer->compression = PARQUET_COMPRESSION_ZSTD;
    parquet_file_writer_set_compression(writer->impl, PARQUET_CODEC_ZSTD);

    return writer;
}

void parquet_writer_close(struct ParquetWriter* writer) {
    if (!writer) return;

    // Close the pure C Parquet writer (writes footer)
    if (writer->impl) {
        parquet_file_writer_close(writer->impl);
        parquet_file_writer_free(writer->impl);
        writer->impl = NULL;
    }

    if (writer->file_path) {
        free(writer->file_path);
    }

    writer->is_open = false;
    free(writer);
}

bool parquet_writer_write_table(struct ParquetWriter* writer, struct ArrowArrayStream* stream) {
    if (!writer || !writer->is_open || !stream || !writer->impl) return false;

    // Get schema from stream
    struct ArrowSchema schema;
    memset(&schema, 0, sizeof(schema));
    if (stream->get_schema(stream, &schema) != 0) {
        return false;
    }

    // Read and write batches from the stream
    struct ArrowArray batch;
    while (1) {
        memset(&batch, 0, sizeof(batch));
        if (stream->get_next(stream, &batch) != 0) {
            if (schema.release) schema.release(&schema);
            return false;
        }
        if (batch.release == NULL) {
            // End of stream
            break;
        }

        // Write this batch
        if (parquet_file_writer_write_batch(writer->impl, &batch, &schema) != 0) {
            if (batch.release) batch.release(&batch);
            if (schema.release) schema.release(&schema);
            return false;
        }

        if (batch.release) batch.release(&batch);
    }

    if (schema.release) schema.release(&schema);
    return true;
}

bool parquet_writer_write_batch(struct ParquetWriter* writer, struct ArrowArray* array) {
    if (!writer || !writer->is_open || !array || !writer->impl) return false;

    // Note: This simplified API doesn't have schema info.
    // Use parquet_writer_write_table with an ArrowArrayStream instead.
    // The stream API provides both schema and batch data.
    return false;
}

void parquet_writer_set_compression(struct ParquetWriter* writer, uint32_t compression) {
    if (!writer || !writer->is_open || !writer->impl) return;

    if (compression <= PARQUET_COMPRESSION_ZSTD) {
        writer->compression = compression;
        // Map parquet_wrapper.h compression constants to parquet_writer_impl.h codecs
        ParquetCompressionCodec codec = PARQUET_CODEC_UNCOMPRESSED;
        switch (compression) {
            case 0: codec = PARQUET_CODEC_UNCOMPRESSED; break;
            case 1: codec = PARQUET_CODEC_SNAPPY; break;
            case 2: codec = PARQUET_CODEC_GZIP; break;
            case 3: codec = PARQUET_CODEC_LZ4; break;
            case 4: codec = PARQUET_CODEC_ZSTD; break;
        }
        parquet_file_writer_set_compression(writer->impl, codec);
    }
}

// Parquet File Metadata operations
uint64_t parquet_metadata_get_num_rows(struct ParquetFileMetadata* metadata) {
    if (!metadata) return 0;
    return metadata->num_rows;
}

uint32_t parquet_metadata_get_num_row_groups(struct ParquetFileMetadata* metadata) {
    if (!metadata) return 0;
    return metadata->num_row_groups;
}

uint64_t parquet_metadata_get_file_size(struct ParquetFileMetadata* metadata) {
    if (!metadata) return 0;
    return metadata->file_size;
}

struct ParquetRowGroupMetadata* parquet_metadata_get_row_group(struct ParquetFileMetadata* metadata, uint32_t index) {
    if (!metadata || index >= metadata->num_row_groups) return NULL;
    
    struct ParquetRowGroupMetadata* row_group = malloc(sizeof(struct ParquetRowGroupMetadata));
    if (!row_group) return NULL;
    
    // TODO: Get actual row group metadata
    row_group->cpp_metadata = NULL;  // Placeholder
    row_group->num_rows = 0;
    row_group->num_columns = 0;
    row_group->total_byte_size = 0;
    
    return row_group;
}

void parquet_metadata_release(struct ParquetFileMetadata* metadata) {
    if (!metadata) return;
    free(metadata);
}

// Parquet Row Group Metadata operations
uint64_t parquet_row_group_get_num_rows(struct ParquetRowGroupMetadata* row_group) {
    if (!row_group) return 0;
    return row_group->num_rows;
}

uint32_t parquet_row_group_get_num_columns(struct ParquetRowGroupMetadata* row_group) {
    if (!row_group) return 0;
    return row_group->num_columns;
}

uint64_t parquet_row_group_get_total_byte_size(struct ParquetRowGroupMetadata* row_group) {
    if (!row_group) return 0;
    return row_group->total_byte_size;
}

struct ParquetColumnMetadata* parquet_row_group_get_column(struct ParquetRowGroupMetadata* row_group, uint32_t index) {
    if (!row_group || index >= row_group->num_columns) return NULL;
    
    struct ParquetColumnMetadata* column = malloc(sizeof(struct ParquetColumnMetadata));
    if (!column) return NULL;
    
    // TODO: Get actual column metadata
    column->cpp_metadata = NULL;  // Placeholder
    column->column_name = malloc(32);
    if (column->column_name) {
        snprintf(column->column_name, 32, "column_%u", index);
    }
    column->compression = PARQUET_COMPRESSION_SNAPPY;
    column->encoding = PARQUET_ENCODING_PLAIN;
    
    return column;
}

void parquet_row_group_release(struct ParquetRowGroupMetadata* row_group) {
    if (!row_group) return;
    free(row_group);
}

// Parquet Column Metadata operations
const char* parquet_column_get_name(struct ParquetColumnMetadata* column) {
    if (!column) return NULL;
    return column->column_name;
}

uint32_t parquet_column_get_compression(struct ParquetColumnMetadata* column) {
    if (!column) return PARQUET_COMPRESSION_UNCOMPRESSED;
    return column->compression;
}

uint32_t parquet_column_get_encoding(struct ParquetColumnMetadata* column) {
    if (!column) return PARQUET_ENCODING_PLAIN;
    return column->encoding;
}

void parquet_column_release(struct ParquetColumnMetadata* column) {
    if (!column) return;
    if (column->column_name) {
        free(column->column_name);
    }
    free(column);
}

// Utility functions
bool parquet_is_available(void) {
    return ensure_parquet_initialized();
}

const char* parquet_get_version(void) {
    // TODO: Return actual Parquet library version
    return "1.12.0-dev";
}

#ifdef __cplusplus
}
#endif
