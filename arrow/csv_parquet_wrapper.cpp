// C wrapper implementation for CSV to Parquet conversion
// Compiled with g++ to match Arrow's ABI, exports only C functions

#include "csv_parquet_wrapper.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/result.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <string>
#include <cstring>
#include <memory>
#include <filesystem>
#include <vector>

// Helper to set error message
static void set_error(char** error_msg, const std::string& msg) {
    if (error_msg) {
        *error_msg = strdup(msg.c_str());
    }
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
    return parquet::Compression::SNAPPY;
}

// Create parent directories if needed
static bool ensure_parent_dir(const std::string& file_path, char** error_msg) {
    std::filesystem::path path(file_path);
    std::filesystem::path parent = path.parent_path();
    if (!parent.empty() && !std::filesystem::exists(parent)) {
        std::error_code ec;
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            set_error(error_msg, "Failed to create directory: " + parent.string());
            return false;
        }
    }
    return true;
}

extern "C" {

int csv_to_parquet_c(
    const char* csv_data,
    const char* output_path,
    const char* compression,
    char** error_msg
) {
    if (!csv_data || !output_path) {
        set_error(error_msg, "csv_data and output_path are required");
        return 1;
    }

    if (strlen(csv_data) == 0) {
        set_error(error_msg, "Empty CSV data");
        return 1;
    }

    if (!ensure_parent_dir(output_path, error_msg)) {
        return 1;
    }

    // Create Arrow buffer from CSV string
    auto csv_buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(csv_data),
        static_cast<int64_t>(strlen(csv_data))
    );
    auto input = std::make_shared<arrow::io::BufferReader>(csv_buffer);

    // Configure CSV reader
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
        set_error(error_msg, "Failed to create CSV reader: " + maybe_reader.status().ToString());
        return 1;
    }

    // Read CSV into Arrow Table
    auto maybe_table = (*maybe_reader)->Read();
    if (!maybe_table.ok()) {
        set_error(error_msg, "Failed to read CSV: " + maybe_table.status().ToString());
        return 1;
    }

    std::shared_ptr<arrow::Table> table = *maybe_table;

    if (table->num_rows() == 0) {
        set_error(error_msg, "CSV contains no data rows");
        return 1;
    }

    // Create output file
    auto maybe_outfile = arrow::io::FileOutputStream::Open(output_path);
    if (!maybe_outfile.ok()) {
        set_error(error_msg, "Failed to open output file: " + maybe_outfile.status().ToString());
        return 1;
    }

    // Configure Parquet writer
    auto builder = parquet::WriterProperties::Builder();
    builder.compression(string_to_compression(compression));
    auto props = builder.build();

    // Write to Parquet
    auto status = parquet::arrow::WriteTable(
        *table,
        arrow::default_memory_pool(),
        *maybe_outfile,
        table->num_rows(),
        props
    );

    if (!status.ok()) {
        set_error(error_msg, "Failed to write Parquet: " + status.ToString());
        return 1;
    }

    return 0;
}

int merge_csv_to_parquet_c(
    const char** csv_strings,
    int csv_count,
    const char* output_path,
    const char* compression,
    char** error_msg
) {
    if (!csv_strings || csv_count <= 0 || !output_path) {
        set_error(error_msg, "Invalid arguments");
        return 1;
    }

    if (!ensure_parent_dir(output_path, error_msg)) {
        return 1;
    }

    std::vector<std::shared_ptr<arrow::Table>> tables;
    tables.reserve(csv_count);

    // Read each CSV
    for (int i = 0; i < csv_count; i++) {
        const char* csv_data = csv_strings[i];
        if (!csv_data || strlen(csv_data) == 0) {
            continue;
        }

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
            set_error(error_msg, "Failed to create CSV reader for chunk " + std::to_string(i));
            return 1;
        }

        auto maybe_table = (*maybe_reader)->Read();
        if (!maybe_table.ok()) {
            set_error(error_msg, "Failed to read CSV chunk " + std::to_string(i));
            return 1;
        }

        if ((*maybe_table)->num_rows() > 0) {
            tables.push_back(*maybe_table);
        }
    }

    if (tables.empty()) {
        set_error(error_msg, "No valid CSV data");
        return 1;
    }

    // Concatenate tables
    auto maybe_combined = arrow::ConcatenateTables(tables);
    if (!maybe_combined.ok()) {
        set_error(error_msg, "Failed to concatenate tables: " + maybe_combined.status().ToString());
        return 1;
    }

    // Create output file
    auto maybe_outfile = arrow::io::FileOutputStream::Open(output_path);
    if (!maybe_outfile.ok()) {
        set_error(error_msg, "Failed to open output file");
        return 1;
    }

    // Write to Parquet
    auto builder = parquet::WriterProperties::Builder();
    builder.compression(string_to_compression(compression));
    auto props = builder.build();

    auto status = parquet::arrow::WriteTable(
        **maybe_combined,
        arrow::default_memory_pool(),
        *maybe_outfile,
        (*maybe_combined)->num_rows(),
        props
    );

    if (!status.ok()) {
        set_error(error_msg, "Failed to write Parquet");
        return 1;
    }

    return 0;
}

int arrow_parquet_available_c(void) {
    return 1;
}

const char* arrow_version_c(void) {
    static std::string version = arrow::GetBuildInfo().version_string;
    return version.c_str();
}

} // extern "C"
