#include "arrow_c_abi.h"
#include <stdlib.h>
#include <string.h>

struct ArrowBuffer* arrow_allocate_buffer(size_t size) {
    struct ArrowBuffer* buffer = (struct ArrowBuffer*)malloc(sizeof(struct ArrowBuffer));
    if (!buffer) return NULL;
    
    buffer->data = (uint8_t*)malloc(size);
    if (!buffer->data && size > 0) {
        free(buffer);
        return NULL;
    }
    
    buffer->size = size;
    buffer->capacity = size;
    
    // Initialize buffer to zero
    if (buffer->data && size > 0) {
        memset(buffer->data, 0, size);
    }
    
    return buffer;
}

int arrow_buffer_resize(struct ArrowBuffer* buffer, size_t new_size) {
    if (!buffer) return -1;
    
    if (new_size <= buffer->capacity) {
        // Just update the size if we're shrinking within capacity
        buffer->size = new_size;
        return 0;
    }
    
    // Need to reallocate
    uint8_t* new_data = (uint8_t*)realloc(buffer->data, new_size);
    if (!new_data && new_size > 0) {
        return -1; // Reallocation failed
    }
    
    // If we expanded, zero out the new portion
    if (new_size > buffer->size) {
        memset(new_data + buffer->size, 0, new_size - buffer->size);
    }
    
    buffer->data = new_data;
    buffer->size = new_size;
    buffer->capacity = new_size;
    
    return 0;
}

void arrow_buffer_free(struct ArrowBuffer* buffer) {
    if (!buffer) return;
    
    free(buffer->data);
    free(buffer);
}

// Additional utility functions for buffer management

int arrow_buffer_write(struct ArrowBuffer* buffer, size_t offset, const void* data, size_t data_size) {
    if (!buffer || !data || offset + data_size > buffer->size) {
        return -1;
    }
    
    memcpy(buffer->data + offset, data, data_size);
    return 0;
}

int arrow_buffer_read(struct ArrowBuffer* buffer, size_t offset, void* data, size_t data_size) {
    if (!buffer || !data || offset + data_size > buffer->size) {
        return -1;
    }
    
    memcpy(data, buffer->data + offset, data_size);
    return 0;
}

size_t arrow_buffer_get_size(struct ArrowBuffer* buffer) {
    return buffer ? buffer->size : 0;
}

size_t arrow_buffer_get_capacity(struct ArrowBuffer* buffer) {
    return buffer ? buffer->capacity : 0;
}

uint8_t* arrow_buffer_get_data(struct ArrowBuffer* buffer) {
    return buffer ? buffer->data : NULL;
}