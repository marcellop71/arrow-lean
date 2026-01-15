#include "arrow_wrapper.h"
#include <lean/lean.h>

// Finalizer functions that are called when Lean objects are garbage collected

extern void lean_arrow_schema_finalizer(void* ptr) {
    if (ptr) {
        struct ArrowSchema* schema = (struct ArrowSchema*)ptr;
        arrow_schema_release(schema);
    }
}

extern void lean_arrow_array_finalizer(void* ptr) {
    if (ptr) {
        struct ArrowArray* array = (struct ArrowArray*)ptr;
        arrow_array_release(array);
    }
}

extern void lean_arrow_stream_finalizer(void* ptr) {
    if (ptr) {
        struct ArrowArrayStream* stream = (struct ArrowArrayStream*)ptr;
        if (stream->release) {
            stream->release(stream);
        }
        free(stream);
    }
}

extern void lean_arrow_buffer_finalizer(void* ptr) {
    if (ptr) {
        struct ArrowBuffer* buffer = (struct ArrowBuffer*)ptr;
        arrow_buffer_free(buffer);
    }
}