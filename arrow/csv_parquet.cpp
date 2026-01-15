// CSV to Parquet conversion using Apache Arrow C++ library
// This file provides native CSV parsing and Parquet writing functionality

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/result.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#include <lean/lean.h>

#include <string>
#include <cstring>
#include <memory>
#include <filesystem>

extern "C" {

// Helper to create Lean error result
static lean_obj_res make_error_result(const char* error_msg) {
    lean_object* except = lean_alloc_ctor(0, 1, 0);  // Except.error
    lean_ctor_set(except, 0, lean_mk_string(error_msg));
    return lean_io_result_mk_ok(except);
}

// Helper to create Lean success result
static lean_obj_res make_ok_result() {
    lean_object* except = lean_alloc_ctor(1, 1, 0);  // Except.ok
    lean_ctor_set(except, 0, lean_box(0));  // Unit
    return lean_io_result_mk_ok(except);
}

// Convert compression string to parquet compression type
static parquet::Compression::type string_to_compression(const char* compression) {
    if (!compression) return parquet::Compression::SNAPPY;

    std::string comp(compression);
    if (comp == "snappy" || comp == "SNAPPY") {
        return parquet::Compression::SNAPPY;
    } else if (comp == "gzip" || comp == "GZIP") {
        return parquet::Compression::GZIP;
    } else if (comp == "zstd" || comp == "ZSTD") {
        return parquet::Compression::ZSTD;
    } else if (comp == "lz4" || comp == "LZ4") {
        return parquet::Compression::LZ4;
    } else if (comp == "brotli" || comp == "BROTLI") {
        return parquet::Compression::BROTLI;
    } else if (comp == "none" || comp == "NONE" || comp == "uncompressed" || comp == "UNCOMPRESSED") {
        return parquet::Compression::UNCOMPRESSED;
    }
    return parquet::Compression::SNAPPY;  // Default
}

// Create parent directories if they don't exist
static bool ensure_parent_dir(const std::string& file_path) {
    std::filesystem::path path(file_path);
    std::filesystem::path parent = path.parent_path();
    if (!parent.empty() && !std::filesystem::exists(parent)) {
        std::error_code ec;
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            return false;
        }
    }
    return true;
}

// Main CSV to Parquet conversion function
// Takes CSV string data and writes to Parquet file
LEAN_EXPORT lean_obj_res lean_csv_to_parquet(
    b_lean_obj_arg csv_data_obj,
    b_lean_obj_arg output_path_obj,
    b_lean_obj_arg compression_obj,
    lean_obj_arg w
) {
    const char* csv_data = lean_string_cstr(csv_data_obj);
    const char* output_path = lean_string_cstr(output_path_obj);
    const char* compression = lean_string_cstr(compression_obj);

    // Ensure parent directory exists
    if (!ensure_parent_dir(output_path)) {
        return make_error_result("Failed to create output directory");
    }

    // Create an Arrow input stream from the CSV string
    auto csv_buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(csv_data),
        static_cast<int64_t>(strlen(csv_data))
    );
    auto input = std::make_shared<arrow::io::BufferReader>(csv_buffer);

    // Configure CSV read options
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Create CSV table reader
    auto maybe_reader = arrow::csv::TableReader::Make(
        arrow::io::default_io_context(),
        input,
        read_options,
        parse_options,
        convert_options
    );

    if (!maybe_reader.ok()) {
        std::string error = "Failed to create CSV reader: " + maybe_reader.status().ToString();
        return make_error_result(error.c_str());
    }

    auto reader = *maybe_reader;

    // Read the CSV into an Arrow Table
    auto maybe_table = reader->Read();
    if (!maybe_table.ok()) {
        std::string error = "Failed to read CSV data: " + maybe_table.status().ToString();
        return make_error_result(error.c_str());
    }

    std::shared_ptr<arrow::Table> table = *maybe_table;

    // Check if table is empty
    if (table->num_rows() == 0) {
        return make_error_result("CSV data is empty or contains only headers");
    }

    // Create output file
    auto maybe_outfile = arrow::io::FileOutputStream::Open(output_path);
    if (!maybe_outfile.ok()) {
        std::string error = "Failed to open output file: " + maybe_outfile.status().ToString();
        return make_error_result(error.c_str());
    }

    auto outfile = *maybe_outfile;

    // Configure Parquet writer properties
    auto builder = parquet::WriterProperties::Builder();
    builder.compression(string_to_compression(compression));
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Write the table to Parquet
    auto status = parquet::arrow::WriteTable(
        *table,
        arrow::default_memory_pool(),
        outfile,
        table->num_rows(),  // row_group_size
        props
    );

    if (!status.ok()) {
        std::string error = "Failed to write Parquet file: " + status.ToString();
        return make_error_result(error.c_str());
    }

    return make_ok_result();
}

// Merge multiple CSV strings and write to Parquet
LEAN_EXPORT lean_obj_res lean_merge_csv_to_parquet(
    b_lean_obj_arg csv_array_obj,
    b_lean_obj_arg output_path_obj,
    b_lean_obj_arg compression_obj,
    lean_obj_arg w
) {
    const char* output_path = lean_string_cstr(output_path_obj);
    const char* compression = lean_string_cstr(compression_obj);

    // Ensure parent directory exists
    if (!ensure_parent_dir(output_path)) {
        return make_error_result("Failed to create output directory");
    }

    size_t num_csvs = lean_array_size(csv_array_obj);
    if (num_csvs == 0) {
        return make_ok_result();  // Nothing to do
    }

    std::vector<std::shared_ptr<arrow::Table>> tables;
    tables.reserve(num_csvs);

    // Read each CSV string into a table
    for (size_t i = 0; i < num_csvs; i++) {
        lean_object* csv_str_obj = lean_array_get_core(csv_array_obj, i);
        const char* csv_data = lean_string_cstr(csv_str_obj);

        auto csv_buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(csv_data),
            static_cast<int64_t>(strlen(csv_data))
        );
        auto input = std::make_shared<arrow::io::BufferReader>(csv_buffer);

        auto read_options = arrow::csv::ReadOptions::Defaults();
        auto parse_options = arrow::csv::ParseOptions::Defaults();
        auto convert_options = arrow::csv::ConvertOptions::Defaults();

        auto maybe_reader = arrow::csv::TableReader::Make(
            arrow::io::default_io_context(),
            input,
            read_options,
            parse_options,
            convert_options
        );

        if (!maybe_reader.ok()) {
            std::string error = "Failed to create CSV reader for chunk " + std::to_string(i) + ": " + maybe_reader.status().ToString();
            return make_error_result(error.c_str());
        }

        auto reader = *maybe_reader;
        auto maybe_table = reader->Read();
        if (!maybe_table.ok()) {
            std::string error = "Failed to read CSV chunk " + std::to_string(i) + ": " + maybe_table.status().ToString();
            return make_error_result(error.c_str());
        }

        if ((*maybe_table)->num_rows() > 0) {
            tables.push_back(*maybe_table);
        }
    }

    if (tables.empty()) {
        return make_error_result("All CSV data is empty");
    }

    // Concatenate all tables
    auto maybe_combined = arrow::ConcatenateTables(tables);
    if (!maybe_combined.ok()) {
        std::string error = "Failed to concatenate tables: " + maybe_combined.status().ToString();
        return make_error_result(error.c_str());
    }

    std::shared_ptr<arrow::Table> combined = *maybe_combined;

    // Create output file
    auto maybe_outfile = arrow::io::FileOutputStream::Open(output_path);
    if (!maybe_outfile.ok()) {
        std::string error = "Failed to open output file: " + maybe_outfile.status().ToString();
        return make_error_result(error.c_str());
    }

    auto outfile = *maybe_outfile;

    // Configure Parquet writer properties
    auto builder = parquet::WriterProperties::Builder();
    builder.compression(string_to_compression(compression));
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Write the combined table to Parquet
    auto status = parquet::arrow::WriteTable(
        *combined,
        arrow::default_memory_pool(),
        outfile,
        combined->num_rows(),
        props
    );

    if (!status.ok()) {
        std::string error = "Failed to write Parquet file: " + status.ToString();
        return make_error_result(error.c_str());
    }

    return make_ok_result();
}

// Check if Arrow/Parquet native support is available
LEAN_EXPORT lean_obj_res lean_arrow_parquet_available(lean_obj_arg w) {
    return lean_io_result_mk_ok(lean_box(1));  // true
}

// Get Arrow library version
LEAN_EXPORT lean_obj_res lean_arrow_version(lean_obj_arg w) {
    std::string version = arrow::GetBuildInfo().version_string;
    return lean_io_result_mk_ok(lean_mk_string(version.c_str()));
}

} // extern "C"
