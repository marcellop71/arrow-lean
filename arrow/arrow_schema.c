#include "arrow_c_abi.h"
#include <stdlib.h>
#include <string.h>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

// Fallback implementation for strdup if not available
#ifndef strdup
static char* strdup(const char* s) {
    size_t len = strlen(s) + 1;
    char* copy = malloc(len);
    if (copy) {
        memcpy(copy, s, len);
    }
    return copy;
}
#endif

// Private data structure for schema
struct ArrowSchemaPrivate {
    char* format_copy;
    char* name_copy;
    char* metadata_copy;
    struct ArrowSchema** children_storage;
};

static void arrow_schema_release_internal(struct ArrowSchema* schema) {
    if (!schema || !schema->release) {
        return;
    }
    
    struct ArrowSchemaPrivate* private_data = (struct ArrowSchemaPrivate*)schema->private_data;
    if (private_data) {
        free(private_data->format_copy);
        free(private_data->name_copy);
        free(private_data->metadata_copy);
        
        // Release children
        if (private_data->children_storage) {
            for (int64_t i = 0; i < schema->n_children; i++) {
                if (private_data->children_storage[i] && private_data->children_storage[i]->release) {
                    private_data->children_storage[i]->release(private_data->children_storage[i]);
                }
                free(private_data->children_storage[i]);
            }
            free(private_data->children_storage);
        }
        
        // Release dictionary
        if (schema->dictionary && schema->dictionary->release) {
            schema->dictionary->release(schema->dictionary);
            free(schema->dictionary);
        }
        
        free(private_data);
    }
    
    schema->release = NULL;
    schema->private_data = NULL;
}

struct ArrowSchema* arrow_schema_init(const char* format) {
    struct ArrowSchema* schema = (struct ArrowSchema*)calloc(1, sizeof(struct ArrowSchema));
    if (!schema) return NULL;
    
    struct ArrowSchemaPrivate* private_data = (struct ArrowSchemaPrivate*)calloc(1, sizeof(struct ArrowSchemaPrivate));
    if (!private_data) {
        free(schema);
        return NULL;
    }
    
    // Copy format string
    private_data->format_copy = strdup(format);
    if (!private_data->format_copy) {
        free(private_data);
        free(schema);
        return NULL;
    }
    
    schema->format = private_data->format_copy;
    schema->name = NULL;
    schema->metadata = NULL;
    schema->flags = 0;
    schema->n_children = 0;
    schema->children = NULL;
    schema->dictionary = NULL;
    schema->release = arrow_schema_release_internal;
    schema->private_data = private_data;
    
    return schema;
}

int arrow_schema_add_child(struct ArrowSchema* schema, struct ArrowSchema* child) {
    if (!schema || !child) return -1;
    
    struct ArrowSchemaPrivate* private_data = (struct ArrowSchemaPrivate*)schema->private_data;
    if (!private_data) return -1;
    
    // Reallocate children array
    struct ArrowSchema** new_children = (struct ArrowSchema**)realloc(
        private_data->children_storage, 
        (schema->n_children + 1) * sizeof(struct ArrowSchema*)
    );
    if (!new_children) return -1;
    
    private_data->children_storage = new_children;
    private_data->children_storage[schema->n_children] = child;
    schema->children = private_data->children_storage;
    schema->n_children++;
    
    return 0;
}

void arrow_schema_release(struct ArrowSchema* schema) {
    if (schema && schema->release) {
        schema->release(schema);
        free(schema);
    }
}