/**
 * arrow_chunked.c - ChunkedArray and Table implementation
 */

#include "arrow_chunked.h"
#include "arrow_wrapper.h"
#include <stdlib.h>
#include <string.h>

// ============================================================================
// Helper: Copy schema
// ============================================================================

static struct ArrowSchema* copy_schema(const struct ArrowSchema* src) {
    if (!src) return NULL;

    struct ArrowSchema* copy = arrow_schema_init(src->format);
    if (!copy) return NULL;

    // Copy name if present
    if (src->name) {
        // We need to access the private data to set the name properly
        // For now, just use the schema's name pointer
    }

    // Copy flags
    copy->flags = src->flags;

    // Copy children recursively
    for (int64_t i = 0; i < src->n_children && src->children; i++) {
        struct ArrowSchema* child_copy = copy_schema(src->children[i]);
        if (child_copy) {
            arrow_schema_add_child(copy, child_copy);
        }
    }

    // Copy dictionary if present
    if (src->dictionary) {
        copy->dictionary = copy_schema(src->dictionary);
    }

    return copy;
}

// ============================================================================
// ChunkedArray Implementation
// ============================================================================

ChunkedArray* chunked_array_create(const struct ArrowSchema* type) {
    if (!type) return NULL;

    ChunkedArray* ca = (ChunkedArray*)calloc(1, sizeof(ChunkedArray));
    if (!ca) return NULL;

    ca->type = copy_schema(type);
    if (!ca->type) {
        free(ca);
        return NULL;
    }

    ca->chunks = NULL;
    ca->num_chunks = 0;
    ca->chunks_capacity = 0;
    ca->total_length = 0;
    ca->null_count = 0;

    return ca;
}

ChunkedArray* chunked_array_from_array(struct ArrowArray* array, const struct ArrowSchema* type) {
    if (!array || !type) return NULL;

    ChunkedArray* ca = chunked_array_create(type);
    if (!ca) return NULL;

    if (chunked_array_add_chunk(ca, array) != 0) {
        chunked_array_free(ca);
        return NULL;
    }

    return ca;
}

int chunked_array_add_chunk(ChunkedArray* ca, struct ArrowArray* chunk) {
    if (!ca || !chunk) return -1;

    // Grow chunks array if needed
    if (ca->num_chunks >= ca->chunks_capacity) {
        size_t new_capacity = ca->chunks_capacity == 0 ? 4 : ca->chunks_capacity * 2;
        struct ArrowArray** new_chunks = (struct ArrowArray**)realloc(
            ca->chunks, new_capacity * sizeof(struct ArrowArray*));
        if (!new_chunks) return -1;
        ca->chunks = new_chunks;
        ca->chunks_capacity = new_capacity;
    }

    ca->chunks[ca->num_chunks] = chunk;
    ca->num_chunks++;
    ca->total_length += chunk->length;
    ca->null_count += chunk->null_count;

    return 0;
}

struct ArrowArray* chunked_array_get_chunk(ChunkedArray* ca, size_t index) {
    if (!ca || index >= ca->num_chunks) return NULL;
    return ca->chunks[index];
}

size_t chunked_array_num_chunks(ChunkedArray* ca) {
    return ca ? ca->num_chunks : 0;
}

int64_t chunked_array_length(ChunkedArray* ca) {
    return ca ? ca->total_length : 0;
}

int64_t chunked_array_null_count(ChunkedArray* ca) {
    return ca ? ca->null_count : 0;
}

struct ArrowSchema* chunked_array_type(ChunkedArray* ca) {
    return ca ? ca->type : NULL;
}

ChunkedArray* chunked_array_slice(ChunkedArray* ca, int64_t offset, int64_t length) {
    if (!ca || offset < 0 || length < 0 || offset >= ca->total_length) {
        return NULL;
    }

    // Clamp length
    if (offset + length > ca->total_length) {
        length = ca->total_length - offset;
    }

    ChunkedArray* result = chunked_array_create(ca->type);
    if (!result) return NULL;

    // Find starting chunk and offset within it
    int64_t current_offset = 0;
    size_t start_chunk = 0;
    int64_t chunk_offset = 0;

    for (size_t i = 0; i < ca->num_chunks; i++) {
        if (current_offset + ca->chunks[i]->length > offset) {
            start_chunk = i;
            chunk_offset = offset - current_offset;
            break;
        }
        current_offset += ca->chunks[i]->length;
    }

    // Copy chunk references (for now, we don't actually slice individual chunks)
    // A proper implementation would create slice views of the arrays
    int64_t remaining = length;
    for (size_t i = start_chunk; i < ca->num_chunks && remaining > 0; i++) {
        struct ArrowArray* chunk = ca->chunks[i];
        int64_t available = chunk->length - (i == start_chunk ? chunk_offset : 0);
        int64_t to_take = available < remaining ? available : remaining;

        // For simplicity, we just reference the whole chunk
        // A proper implementation would create array slices
        if (chunked_array_add_chunk(result, chunk) != 0) {
            // Note: We're not properly handling ownership here
            chunked_array_free(result);
            return NULL;
        }
        remaining -= to_take;
    }

    return result;
}

void chunked_array_free(ChunkedArray* ca) {
    if (!ca) return;

    // Release all chunks
    if (ca->chunks) {
        for (size_t i = 0; i < ca->num_chunks; i++) {
            if (ca->chunks[i] && ca->chunks[i]->release) {
                ca->chunks[i]->release(ca->chunks[i]);
                free(ca->chunks[i]);
            }
        }
        free(ca->chunks);
    }

    // Release type schema
    if (ca->type) {
        arrow_schema_release(ca->type);
    }

    free(ca);
}

// ============================================================================
// Table Implementation
// ============================================================================

Table* table_create(const struct ArrowSchema* schema) {
    if (!schema) return NULL;

    Table* table = (Table*)calloc(1, sizeof(Table));
    if (!table) return NULL;

    table->schema = copy_schema(schema);
    if (!table->schema) {
        free(table);
        return NULL;
    }

    table->num_columns = (size_t)schema->n_children;
    if (table->num_columns > 0) {
        table->columns = (ChunkedArray**)calloc(table->num_columns, sizeof(ChunkedArray*));
        if (!table->columns) {
            arrow_schema_release(table->schema);
            free(table);
            return NULL;
        }
    }

    table->num_rows = 0;

    return table;
}

Table* table_from_record_batch(struct ArrowArray* batch, const struct ArrowSchema* schema) {
    if (!batch || !schema) return NULL;

    Table* table = table_create(schema);
    if (!table) return NULL;

    table->num_rows = batch->length;

    // Create chunked arrays for each column
    for (size_t i = 0; i < table->num_columns && i < (size_t)batch->n_children; i++) {
        struct ArrowArray* col_array = batch->children[i];
        struct ArrowSchema* col_schema = schema->children ? schema->children[i] : NULL;

        if (col_array && col_schema) {
            table->columns[i] = chunked_array_from_array(col_array, col_schema);
            if (!table->columns[i]) {
                // Don't release the batch's children since we don't own them
                // Just mark as failed
                table_free(table);
                return NULL;
            }
        }
    }

    return table;
}

Table* table_from_chunked_arrays(ChunkedArray** columns, size_t num_columns, const struct ArrowSchema* schema) {
    if (!columns || !schema) return NULL;

    Table* table = table_create(schema);
    if (!table) return NULL;

    // Verify column count matches
    if (num_columns != table->num_columns) {
        table_free(table);
        return NULL;
    }

    // Transfer ownership of columns
    int64_t num_rows = 0;
    for (size_t i = 0; i < num_columns; i++) {
        table->columns[i] = columns[i];
        if (columns[i] && num_rows == 0) {
            num_rows = chunked_array_length(columns[i]);
        }
    }

    table->num_rows = num_rows;
    return table;
}

Table* table_concatenate(Table** tables, size_t count) {
    if (!tables || count == 0) return NULL;

    // Use first table's schema
    Table* first = tables[0];
    if (!first) return NULL;

    Table* result = table_create(first->schema);
    if (!result) return NULL;

    // Create new chunked arrays by combining chunks from all tables
    for (size_t col = 0; col < result->num_columns; col++) {
        ChunkedArray* combined = NULL;

        for (size_t t = 0; t < count; t++) {
            Table* tbl = tables[t];
            if (!tbl || col >= tbl->num_columns) continue;

            ChunkedArray* src = tbl->columns[col];
            if (!src) continue;

            if (!combined) {
                combined = chunked_array_create(src->type);
                if (!combined) {
                    table_free(result);
                    return NULL;
                }
            }

            // Add all chunks from this table's column
            for (size_t c = 0; c < src->num_chunks; c++) {
                struct ArrowArray* chunk = src->chunks[c];
                if (chunk) {
                    // Note: This doesn't properly copy the chunk
                    // In a real implementation, we'd need to handle ownership carefully
                    if (chunked_array_add_chunk(combined, chunk) != 0) {
                        chunked_array_free(combined);
                        table_free(result);
                        return NULL;
                    }
                }
            }
        }

        result->columns[col] = combined;
    }

    // Calculate total rows
    result->num_rows = 0;
    for (size_t t = 0; t < count; t++) {
        if (tables[t]) {
            result->num_rows += tables[t]->num_rows;
        }
    }

    return result;
}

ChunkedArray* table_get_column(Table* table, size_t index) {
    if (!table || index >= table->num_columns) return NULL;
    return table->columns[index];
}

ChunkedArray* table_get_column_by_name(Table* table, const char* name) {
    if (!table || !name || !table->schema || !table->schema->children) return NULL;

    for (size_t i = 0; i < table->num_columns; i++) {
        struct ArrowSchema* child = table->schema->children[i];
        if (child && child->name && strcmp(child->name, name) == 0) {
            return table->columns[i];
        }
    }

    return NULL;
}

size_t table_num_columns(Table* table) {
    return table ? table->num_columns : 0;
}

int64_t table_num_rows(Table* table) {
    return table ? table->num_rows : 0;
}

struct ArrowSchema* table_schema(Table* table) {
    return table ? table->schema : NULL;
}

const char* table_column_name(Table* table, size_t index) {
    if (!table || !table->schema || !table->schema->children || index >= table->num_columns) {
        return NULL;
    }
    struct ArrowSchema* child = table->schema->children[index];
    return child ? child->name : NULL;
}

Table* table_slice(Table* table, int64_t offset, int64_t length) {
    if (!table || offset < 0 || length < 0 || offset >= table->num_rows) {
        return NULL;
    }

    // Clamp length
    if (offset + length > table->num_rows) {
        length = table->num_rows - offset;
    }

    Table* result = table_create(table->schema);
    if (!result) return NULL;

    result->num_rows = length;

    // Slice each column
    for (size_t i = 0; i < table->num_columns; i++) {
        if (table->columns[i]) {
            result->columns[i] = chunked_array_slice(table->columns[i], offset, length);
            if (!result->columns[i]) {
                table_free(result);
                return NULL;
            }
        }
    }

    return result;
}

Table* table_select_columns(Table* table, const size_t* indices, size_t num_indices) {
    if (!table || !indices || num_indices == 0) return NULL;

    // Build new schema with selected columns
    struct ArrowSchema* new_schema = arrow_schema_init("+s");
    if (!new_schema) return NULL;

    for (size_t i = 0; i < num_indices; i++) {
        size_t idx = indices[i];
        if (idx < table->num_columns && table->schema->children && table->schema->children[idx]) {
            struct ArrowSchema* child_copy = copy_schema(table->schema->children[idx]);
            if (child_copy) {
                arrow_schema_add_child(new_schema, child_copy);
            }
        }
    }

    Table* result = table_create(new_schema);
    arrow_schema_release(new_schema);

    if (!result) return NULL;

    result->num_rows = table->num_rows;

    // Copy selected columns
    for (size_t i = 0; i < num_indices; i++) {
        size_t idx = indices[i];
        if (idx < table->num_columns && table->columns[idx]) {
            // Create a copy of the chunked array (shares chunks)
            result->columns[i] = chunked_array_create(table->columns[idx]->type);
            if (result->columns[i]) {
                for (size_t c = 0; c < table->columns[idx]->num_chunks; c++) {
                    // Note: This doesn't properly copy - shares the chunk pointers
                    chunked_array_add_chunk(result->columns[i], table->columns[idx]->chunks[c]);
                }
            }
        }
    }

    return result;
}

int table_add_column(Table* table, ChunkedArray* column, const char* name) {
    if (!table || !column) return -1;

    // Verify row count matches
    if (table->num_rows > 0 && chunked_array_length(column) != table->num_rows) {
        return -1;
    }

    // Grow columns array
    size_t new_count = table->num_columns + 1;
    ChunkedArray** new_columns = (ChunkedArray**)realloc(
        table->columns, new_count * sizeof(ChunkedArray*));
    if (!new_columns) return -1;

    table->columns = new_columns;
    table->columns[table->num_columns] = column;

    // Add column schema to table schema
    struct ArrowSchema* col_schema = copy_schema(column->type);
    if (col_schema && name) {
        // Set name (simplified - would need proper private data handling)
    }
    if (col_schema) {
        arrow_schema_add_child(table->schema, col_schema);
    }

    table->num_columns = new_count;

    // Update row count if this is the first column
    if (table->num_rows == 0) {
        table->num_rows = chunked_array_length(column);
    }

    return 0;
}

void table_free(Table* table) {
    if (!table) return;

    // Free all columns
    if (table->columns) {
        for (size_t i = 0; i < table->num_columns; i++) {
            if (table->columns[i]) {
                chunked_array_free(table->columns[i]);
            }
        }
        free(table->columns);
    }

    // Free schema
    if (table->schema) {
        arrow_schema_release(table->schema);
    }

    free(table);
}

// Note: RecordBatch convenience functions are defined in arrow_builders.c
