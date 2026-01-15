#include "arrow_c_abi.h"
#include <stdlib.h>
#include <string.h>

// Private data structure for array
struct ArrowArrayPrivate {
    const void** buffers_storage;
    struct ArrowArray** children_storage;
};

static void arrow_array_release_internal(struct ArrowArray* array) {
    if (!array || !array->release) {
        return;
    }
    
    struct ArrowArrayPrivate* private_data = (struct ArrowArrayPrivate*)array->private_data;
    if (private_data) {
        // Free buffers - note: we don't free the actual buffer data as it might be managed elsewhere
        free(private_data->buffers_storage);
        
        // Release children
        if (private_data->children_storage) {
            for (int64_t i = 0; i < array->n_children; i++) {
                if (private_data->children_storage[i] && private_data->children_storage[i]->release) {
                    private_data->children_storage[i]->release(private_data->children_storage[i]);
                }
                free(private_data->children_storage[i]);
            }
            free(private_data->children_storage);
        }
        
        // Release dictionary
        if (array->dictionary && array->dictionary->release) {
            array->dictionary->release(array->dictionary);
            free(array->dictionary);
        }
        
        free(private_data);
    }
    
    array->release = NULL;
    array->private_data = NULL;
}

struct ArrowArray* arrow_array_init(int64_t length) {
    struct ArrowArray* array = (struct ArrowArray*)calloc(1, sizeof(struct ArrowArray));
    if (!array) return NULL;
    
    struct ArrowArrayPrivate* private_data = (struct ArrowArrayPrivate*)calloc(1, sizeof(struct ArrowArrayPrivate));
    if (!private_data) {
        free(array);
        return NULL;
    }
    
    array->length = length;
    array->null_count = 0;
    array->offset = 0;
    array->n_buffers = 0;
    array->n_children = 0;
    array->buffers = NULL;
    array->children = NULL;
    array->dictionary = NULL;
    array->release = arrow_array_release_internal;
    array->private_data = private_data;
    
    return array;
}

int arrow_array_set_buffer(struct ArrowArray* array, size_t index, uint8_t* buffer) {
    if (!array) return -1;
    
    struct ArrowArrayPrivate* private_data = (struct ArrowArrayPrivate*)array->private_data;
    if (!private_data) return -1;
    
    // Ensure we have enough buffer slots
    if ((int64_t)index >= array->n_buffers) {
        size_t new_size = index + 1;
        const void** new_buffers = (const void**)realloc(
            private_data->buffers_storage, 
            new_size * sizeof(void*)
        );
        if (!new_buffers) return -1;
        
        // Initialize new slots to NULL
        for (size_t i = array->n_buffers; i < new_size; i++) {
            new_buffers[i] = NULL;
        }
        
        private_data->buffers_storage = new_buffers;
        array->buffers = private_data->buffers_storage;
        array->n_buffers = new_size;
    }
    
    private_data->buffers_storage[index] = buffer;
    return 0;
}

uint8_t* arrow_array_get_buffer(struct ArrowArray* array, size_t index) {
    if (!array || (int64_t)index >= array->n_buffers) return NULL;
    
    return (uint8_t*)array->buffers[index];
}

void arrow_array_release(struct ArrowArray* array) {
    if (array && array->release) {
        array->release(array);
        free(array);
    }
}