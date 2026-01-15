/**
 * arrow_chunked.h - ChunkedArray and Table for Arrow
 *
 * ChunkedArray: Represents a single column as a sequence of contiguous chunks.
 * Table: Represents a table as a collection of ChunkedArray columns.
 */

#ifndef ARROW_CHUNKED_H
#define ARROW_CHUNKED_H

#include "arrow_c_abi.h"
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// ChunkedArray
// ============================================================================

/**
 * ChunkedArray represents a column as a sequence of contiguous chunks.
 * All chunks must have the same type.
 */
typedef struct ChunkedArray {
    struct ArrowArray** chunks;   // Array of chunk pointers
    size_t num_chunks;            // Number of chunks
    size_t chunks_capacity;       // Capacity of chunks array
    int64_t total_length;         // Total number of elements across all chunks
    int64_t null_count;           // Total null count across all chunks
    struct ArrowSchema* type;     // Schema describing the type (owned)
} ChunkedArray;

/**
 * Create a new ChunkedArray with the given type.
 * @param type Schema describing the type (will be copied)
 * @return New ChunkedArray or NULL on failure
 */
ChunkedArray* chunked_array_create(const struct ArrowSchema* type);

/**
 * Create a ChunkedArray from a single array.
 * @param array The array to wrap (ownership transferred)
 * @param type Schema describing the type (will be copied)
 * @return New ChunkedArray or NULL on failure
 */
ChunkedArray* chunked_array_from_array(struct ArrowArray* array, const struct ArrowSchema* type);

/**
 * Add a chunk to the ChunkedArray.
 * @param ca The ChunkedArray
 * @param chunk The chunk to add (ownership transferred)
 * @return 0 on success, -1 on failure
 */
int chunked_array_add_chunk(ChunkedArray* ca, struct ArrowArray* chunk);

/**
 * Get a chunk by index.
 * @param ca The ChunkedArray
 * @param index The chunk index
 * @return The chunk (not owned) or NULL if index is out of range
 */
struct ArrowArray* chunked_array_get_chunk(ChunkedArray* ca, size_t index);

/**
 * Get the number of chunks.
 */
size_t chunked_array_num_chunks(ChunkedArray* ca);

/**
 * Get the total length across all chunks.
 */
int64_t chunked_array_length(ChunkedArray* ca);

/**
 * Get the total null count across all chunks.
 */
int64_t chunked_array_null_count(ChunkedArray* ca);

/**
 * Get the type schema.
 */
struct ArrowSchema* chunked_array_type(ChunkedArray* ca);

/**
 * Slice a ChunkedArray.
 * @param ca The ChunkedArray to slice
 * @param offset Start offset
 * @param length Number of elements to include
 * @return New ChunkedArray containing the slice
 */
ChunkedArray* chunked_array_slice(ChunkedArray* ca, int64_t offset, int64_t length);

/**
 * Free a ChunkedArray and all its chunks.
 */
void chunked_array_free(ChunkedArray* ca);

// ============================================================================
// Table
// ============================================================================

/**
 * Table represents a table as columns of ChunkedArrays.
 */
typedef struct Table {
    struct ArrowSchema* schema;    // Table schema (owned)
    ChunkedArray** columns;        // Array of columns
    size_t num_columns;            // Number of columns
    int64_t num_rows;              // Number of rows
} Table;

/**
 * Create an empty Table with the given schema.
 * @param schema Table schema (will be copied)
 * @return New Table or NULL on failure
 */
Table* table_create(const struct ArrowSchema* schema);

/**
 * Create a Table from a record batch (single-chunk columns).
 * @param batch The record batch (struct array)
 * @param schema The schema for the batch
 * @return New Table or NULL on failure
 */
Table* table_from_record_batch(struct ArrowArray* batch, const struct ArrowSchema* schema);

/**
 * Create a Table from an array of ChunkedArrays.
 * @param columns Array of ChunkedArrays (ownership transferred)
 * @param num_columns Number of columns
 * @param schema Table schema (will be copied)
 * @return New Table or NULL on failure
 */
Table* table_from_chunked_arrays(ChunkedArray** columns, size_t num_columns, const struct ArrowSchema* schema);

/**
 * Concatenate multiple tables vertically.
 * All tables must have the same schema.
 * @param tables Array of tables to concatenate
 * @param count Number of tables
 * @return New concatenated Table or NULL on failure
 */
Table* table_concatenate(Table** tables, size_t count);

/**
 * Get a column by index.
 * @param table The Table
 * @param index Column index
 * @return The ChunkedArray (not owned) or NULL if index is out of range
 */
ChunkedArray* table_get_column(Table* table, size_t index);

/**
 * Get a column by name.
 * @param table The Table
 * @param name Column name
 * @return The ChunkedArray (not owned) or NULL if not found
 */
ChunkedArray* table_get_column_by_name(Table* table, const char* name);

/**
 * Get the number of columns.
 */
size_t table_num_columns(Table* table);

/**
 * Get the number of rows.
 */
int64_t table_num_rows(Table* table);

/**
 * Get the table schema.
 */
struct ArrowSchema* table_schema(Table* table);

/**
 * Get a column name by index.
 */
const char* table_column_name(Table* table, size_t index);

/**
 * Slice a Table (select rows).
 * @param table The Table to slice
 * @param offset Start offset
 * @param length Number of rows to include
 * @return New Table containing the slice
 */
Table* table_slice(Table* table, int64_t offset, int64_t length);

/**
 * Select columns by indices.
 * @param table The Table
 * @param indices Array of column indices
 * @param num_indices Number of indices
 * @return New Table with selected columns
 */
Table* table_select_columns(Table* table, const size_t* indices, size_t num_indices);

/**
 * Add a column to the table.
 * @param table The Table
 * @param column The ChunkedArray to add (ownership transferred)
 * @param name Column name
 * @return 0 on success, -1 on failure
 */
int table_add_column(Table* table, ChunkedArray* column, const char* name);

/**
 * Free a Table and all its columns.
 */
void table_free(Table* table);

// ============================================================================
// RecordBatch (convenience alias for a single-chunk table)
// ============================================================================

/**
 * Create a record batch (struct array) from arrays.
 * @param arrays Array of column arrays (ownership NOT transferred)
 * @param num_arrays Number of arrays
 * @param schema Schema for the record batch
 * @return New struct ArrowArray representing the batch
 */
struct ArrowArray* record_batch_create(struct ArrowArray** arrays, size_t num_arrays, const struct ArrowSchema* schema);

/**
 * Get the schema of a record batch.
 */
struct ArrowSchema* record_batch_schema(struct ArrowArray* batch);

/**
 * Get a column from a record batch.
 */
struct ArrowArray* record_batch_column(struct ArrowArray* batch, size_t index);

/**
 * Get the number of rows in a record batch.
 */
int64_t record_batch_num_rows(struct ArrowArray* batch);

/**
 * Get the number of columns in a record batch.
 */
size_t record_batch_num_columns(struct ArrowArray* batch);

#ifdef __cplusplus
}
#endif

#endif // ARROW_CHUNKED_H
