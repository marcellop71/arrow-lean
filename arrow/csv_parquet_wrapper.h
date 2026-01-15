// C wrapper for CSV to Parquet conversion
// This header exposes a pure C interface - no C++ types leak through
#ifndef CSV_PARQUET_WRAPPER_H
#define CSV_PARQUET_WRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

// Convert CSV string to Parquet file
// Returns 0 on success, non-zero on error
// If error_msg is not NULL and an error occurs, it will be set to a malloc'd string
// (caller must free it)
int csv_to_parquet_c(
    const char* csv_data,
    const char* output_path,
    const char* compression,  // "snappy", "gzip", "zstd", "lz4", "brotli", "none"
    char** error_msg
);

// Merge multiple CSV strings into a single Parquet file
// csv_strings is an array of C strings, csv_count is the number of strings
// Returns 0 on success, non-zero on error
int merge_csv_to_parquet_c(
    const char** csv_strings,
    int csv_count,
    const char* output_path,
    const char* compression,
    char** error_msg
);

// Check if Arrow/Parquet support is available
// Returns 1 if available, 0 if not
int arrow_parquet_available_c(void);

// Get Arrow version string
// Returns a static string (do not free)
const char* arrow_version_c(void);

#ifdef __cplusplus
}
#endif

#endif // CSV_PARQUET_WRAPPER_H
