#include "arrow_c_abi.h"
#include <stdlib.h>
#include <string.h>

// Private data structure for stream
struct ArrowArrayStreamPrivate {
    struct ArrowSchema* schema;
    struct ArrowArray** arrays;
    size_t array_count;
    size_t current_index;
    char* last_error;
};

static int arrow_stream_get_schema_internal(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
    if (!stream || !out) return -1;
    
    struct ArrowArrayStreamPrivate* private_data = (struct ArrowArrayStreamPrivate*)stream->private_data;
    if (!private_data || !private_data->schema) return -1;
    
    // Copy schema (shallow copy for now)
    memcpy(out, private_data->schema, sizeof(struct ArrowSchema));
    return 0;
}

static int arrow_stream_get_next_internal(struct ArrowArrayStream* stream, struct ArrowArray* out) {
    if (!stream || !out) return -1;
    
    struct ArrowArrayStreamPrivate* private_data = (struct ArrowArrayStreamPrivate*)stream->private_data;
    if (!private_data) return -1;
    
    // Check if we have more arrays
    if (private_data->current_index >= private_data->array_count) {
        // End of stream - set length to 0 to indicate no more data
        memset(out, 0, sizeof(struct ArrowArray));
        return 0;
    }
    
    // Copy the next array
    struct ArrowArray* next_array = private_data->arrays[private_data->current_index];
    if (next_array) {
        memcpy(out, next_array, sizeof(struct ArrowArray));
        private_data->current_index++;
        return 0;
    }
    
    return -1;
}

static const char* arrow_stream_get_last_error_internal(struct ArrowArrayStream* stream) {
    if (!stream) return "Invalid stream";
    
    struct ArrowArrayStreamPrivate* private_data = (struct ArrowArrayStreamPrivate*)stream->private_data;
    if (!private_data) return "Stream not initialized";
    
    return private_data->last_error ? private_data->last_error : "No error";
}

static void arrow_stream_release_internal(struct ArrowArrayStream* stream) {
    if (!stream || !stream->release) {
        return;
    }
    
    struct ArrowArrayStreamPrivate* private_data = (struct ArrowArrayStreamPrivate*)stream->private_data;
    if (private_data) {
        // Release schema
        if (private_data->schema && private_data->schema->release) {
            private_data->schema->release(private_data->schema);
            free(private_data->schema);
        }
        
        // Release arrays
        if (private_data->arrays) {
            for (size_t i = 0; i < private_data->array_count; i++) {
                if (private_data->arrays[i] && private_data->arrays[i]->release) {
                    private_data->arrays[i]->release(private_data->arrays[i]);
                    free(private_data->arrays[i]);
                }
            }
            free(private_data->arrays);
        }
        
        free(private_data->last_error);
        free(private_data);
    }
    
    stream->release = NULL;
    stream->private_data = NULL;
}

struct ArrowArrayStream* arrow_stream_init(void) {
    struct ArrowArrayStream* stream = (struct ArrowArrayStream*)calloc(1, sizeof(struct ArrowArrayStream));
    if (!stream) return NULL;
    
    struct ArrowArrayStreamPrivate* private_data = (struct ArrowArrayStreamPrivate*)calloc(1, sizeof(struct ArrowArrayStreamPrivate));
    if (!private_data) {
        free(stream);
        return NULL;
    }
    
    stream->get_schema = arrow_stream_get_schema_internal;
    stream->get_next = arrow_stream_get_next_internal;
    stream->get_last_error = arrow_stream_get_last_error_internal;
    stream->release = arrow_stream_release_internal;
    stream->private_data = private_data;
    
    private_data->schema = NULL;
    private_data->arrays = NULL;
    private_data->array_count = 0;
    private_data->current_index = 0;
    private_data->last_error = NULL;
    
    return stream;
}

struct ArrowSchema* arrow_stream_get_schema(struct ArrowArrayStream* stream) {
    if (!stream) return NULL;
    
    struct ArrowSchema* schema = (struct ArrowSchema*)malloc(sizeof(struct ArrowSchema));
    if (!schema) return NULL;
    
    if (stream->get_schema(stream, schema) != 0) {
        free(schema);
        return NULL;
    }
    
    return schema;
}

struct ArrowArray* arrow_stream_get_next(struct ArrowArrayStream* stream) {
    if (!stream) return NULL;
    
    struct ArrowArray* array = (struct ArrowArray*)malloc(sizeof(struct ArrowArray));
    if (!array) return NULL;
    
    if (stream->get_next(stream, array) != 0) {
        free(array);
        return NULL;
    }
    
    // Check if this is end of stream (length == 0 and release == NULL)
    if (array->length == 0 && array->release == NULL) {
        free(array);
        return NULL;
    }
    
    return array;
}