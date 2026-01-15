#ifndef PARQUET_WRAPPER_H
#define PARQUET_WRAPPER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "arrow_wrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations for Parquet structures
struct ParquetReader;
struct ParquetWriter;
struct ParquetFileMetadata;
struct ParquetRowGroupMetadata;
struct ParquetColumnMetadata;

// Compression type constants (for legacy API compatibility)
// Note: New code should use ParquetCompressionCodec enum from parquet_writer_impl.h
#define PARQUET_COMPRESSION_UNCOMPRESSED 0
#define PARQUET_COMPRESSION_SNAPPY       1
#define PARQUET_COMPRESSION_GZIP         2
#define PARQUET_COMPRESSION_LZO          3
#define PARQUET_COMPRESSION_BROTLI       4
#define PARQUET_COMPRESSION_LZ4          5
#define PARQUET_COMPRESSION_ZSTD         6

// Note: Encoding constants moved to ParquetEncoding enum in parquet_writer_impl.h
// to avoid macro/enum conflicts

// Parquet Reader operations
struct ParquetReader* parquet_reader_open(const char* file_path);
void parquet_reader_close(struct ParquetReader* reader);
struct ParquetFileMetadata* parquet_reader_get_metadata(struct ParquetReader* reader);
struct ArrowArrayStream* parquet_reader_read_table(struct ParquetReader* reader);
struct ArrowArrayStream* parquet_reader_read_row_group(struct ParquetReader* reader, uint32_t row_group);
struct ArrowArrayStream* parquet_reader_read_columns(struct ParquetReader* reader, const char** columns, size_t num_columns);

// Parquet Writer operations
struct ParquetWriter* parquet_writer_open(const char* file_path, struct ArrowSchema* schema);
void parquet_writer_close(struct ParquetWriter* writer);
bool parquet_writer_write_table(struct ParquetWriter* writer, struct ArrowArrayStream* stream);
bool parquet_writer_write_batch(struct ParquetWriter* writer, struct ArrowArray* array);
void parquet_writer_set_compression(struct ParquetWriter* writer, uint32_t compression);

// Parquet File Metadata operations
uint64_t parquet_metadata_get_num_rows(struct ParquetFileMetadata* metadata);
uint32_t parquet_metadata_get_num_row_groups(struct ParquetFileMetadata* metadata);
uint64_t parquet_metadata_get_file_size(struct ParquetFileMetadata* metadata);
struct ParquetRowGroupMetadata* parquet_metadata_get_row_group(struct ParquetFileMetadata* metadata, uint32_t index);
void parquet_metadata_release(struct ParquetFileMetadata* metadata);

// Parquet Row Group Metadata operations
uint64_t parquet_row_group_get_num_rows(struct ParquetRowGroupMetadata* row_group);
uint32_t parquet_row_group_get_num_columns(struct ParquetRowGroupMetadata* row_group);
uint64_t parquet_row_group_get_total_byte_size(struct ParquetRowGroupMetadata* row_group);
struct ParquetColumnMetadata* parquet_row_group_get_column(struct ParquetRowGroupMetadata* row_group, uint32_t index);
void parquet_row_group_release(struct ParquetRowGroupMetadata* row_group);

// Parquet Column Metadata operations
const char* parquet_column_get_name(struct ParquetColumnMetadata* column);
uint32_t parquet_column_get_compression(struct ParquetColumnMetadata* column);
uint32_t parquet_column_get_encoding(struct ParquetColumnMetadata* column);
void parquet_column_release(struct ParquetColumnMetadata* column);

// Utility functions
bool parquet_is_available(void);
const char* parquet_get_version(void);

#ifdef __cplusplus
}
#endif

#endif // PARQUET_WRAPPER_H
