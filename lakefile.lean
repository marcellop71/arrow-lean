import Lake
open System Lake DSL

-- Link arguments (pure C, no C++ dependencies)
package arrowLean where
  extraDepTargets := #[`libarrow_wrapper]
  moreLinkArgs := #[
    "-L/usr/local/lib",
    "-Wl,-rpath,/usr/local/lib",
    "-Wl,--allow-shlib-undefined",
    "-lzlog"
  ]

@[default_target]
lean_lib ArrowLean

lean_lib Examples where
  globs := #[.submodules `Examples]

target arrow_schema_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "arrow_schema.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "arrow_schema.c") flags
  return .pure oFile

target arrow_array_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "arrow_array.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "arrow_array.c") flags
  return .pure oFile

target arrow_stream_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "arrow_stream.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "arrow_stream.c") flags
  return .pure oFile

target arrow_data_access_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "arrow_data_access.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "arrow_data_access.c") flags
  return .pure oFile

target arrow_buffer_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "arrow_buffer.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "arrow_buffer.c") flags
  return .pure oFile

target lean_arrow_wrapper_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "lean_arrow_wrapper.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "lean_arrow_wrapper.c") flags
  return .pure oFile

target lean_arrow_finalizers_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "lean_arrow_finalizers.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "lean_arrow_finalizers.c") flags
  return .pure oFile

target lean_parquet_wrapper_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "lean_parquet_wrapper.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "lean_parquet_wrapper.c") flags
  return .pure oFile

target parquet_reader_writer_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "parquet_reader_writer.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "parquet_reader_writer.c") flags
  return .pure oFile

-- Pure C Parquet writer implementation (Thrift serialization, data pages, footer)
target parquet_writer_impl_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "parquet_writer_impl.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "parquet_writer_impl.c") flags
  return .pure oFile

-- Pure C Parquet reader implementation (Thrift decoding, page reading)
target parquet_reader_impl_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "parquet_reader_impl.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "parquet_reader_impl.c") flags
  return .pure oFile

target arrow_ipc_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "arrow_ipc.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "arrow_ipc.c") flags
  return .pure oFile

target lean_arrow_ipc_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "lean_arrow_ipc.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "lean_arrow_ipc.c") flags
  return .pure oFile

-- Typed column builders for direct Arrow array construction (pure C)
target arrow_builders_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "arrow_builders.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "arrow_builders.c") flags
  return .pure oFile

-- Lean FFI wrappers for typed builders (pure C)
target lean_builder_wrapper_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "lean_builder_wrapper.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "lean_builder_wrapper.c") flags
  return .pure oFile

-- Nested type builders (List, Struct, Decimal128, Dictionary, Map)
target arrow_nested_builders_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "arrow_nested_builders.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "arrow_nested_builders.c") flags
  return .pure oFile

-- Arrow compute functions (arithmetic, comparisons, aggregations)
target arrow_compute_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "arrow_compute.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "arrow_compute.c") flags
  return .pure oFile

-- Lean FFI wrappers for Arrow compute functions
target lean_arrow_compute_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "lean_arrow_compute.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "lean_arrow_compute.c") flags
  return .pure oFile

-- ChunkedArray and Table implementation
target arrow_chunked_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "arrow_chunked.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "arrow_chunked.c") flags
  return .pure oFile

-- Lean FFI wrappers for ChunkedArray and Table
target lean_arrow_chunked_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "lean_arrow_chunked.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString, "-I", (pkg.dir / "arrow").toString]
  compileO oFile (pkg.dir / "arrow" / "lean_arrow_chunked.c") flags
  return .pure oFile

-- Stub for CSV/Parquet functions (C++ dependency removed)
-- Use the new typed builders + IPC for data serialization instead
target csv_parquet_stub_o pkg : FilePath := do
  let oFile := pkg.buildDir / "arrow" / "csv_parquet_stub.o"
  IO.FS.createDirAll oFile.parent.get!
  let stubSrc := pkg.buildDir / "arrow" / "csv_parquet_stub.c"
  IO.FS.writeFile stubSrc "
#include <lean/lean.h>

// CSV to Parquet functionality has been replaced with typed builders
// Use ArrowLean.TypedBuilders and ArrowLean.ToArrow for direct Arrow construction
// Use ArrowLean.IPC for serialization (pure C implementation)

LEAN_EXPORT lean_obj_res lean_csv_to_parquet(b_lean_obj_arg csv, b_lean_obj_arg path, b_lean_obj_arg comp, lean_obj_arg w) {
    lean_object* except = lean_alloc_ctor(0, 1, 0);
    lean_ctor_set(except, 0, lean_mk_string(\"CSV to Parquet via C++ removed. Use TypedBuilders + IPC instead.\"));
    return lean_io_result_mk_ok(except);
}

LEAN_EXPORT lean_obj_res lean_merge_csv_to_parquet(b_lean_obj_arg csvs, b_lean_obj_arg path, b_lean_obj_arg comp, lean_obj_arg w) {
    lean_object* except = lean_alloc_ctor(0, 1, 0);
    lean_ctor_set(except, 0, lean_mk_string(\"CSV to Parquet via C++ removed. Use TypedBuilders + IPC instead.\"));
    return lean_io_result_mk_ok(except);
}

LEAN_EXPORT lean_obj_res lean_arrow_parquet_available(lean_obj_arg w) {
    // Return false - C++ Parquet support removed
    return lean_io_result_mk_ok(lean_box(0));
}

LEAN_EXPORT lean_obj_res lean_arrow_version(lean_obj_arg w) {
    return lean_io_result_mk_ok(lean_mk_string(\"pure-c-1.0.0\"));
}
"
  let flags := #["-fPIC", "-O2", "-std=c99", "-I", (← getLeanIncludeDir).toString]
  compileO oFile stubSrc flags
  return .pure oFile

extern_lib libarrow_wrapper pkg := do
  -- Core Arrow C implementation
  let schemaObj ← arrow_schema_o.fetch
  let arrayObj ← arrow_array_o.fetch
  let streamObj ← arrow_stream_o.fetch
  let dataAccessObj ← arrow_data_access_o.fetch
  let bufferObj ← arrow_buffer_o.fetch
  let wrapperObj ← lean_arrow_wrapper_o.fetch
  let finalizersObj ← lean_arrow_finalizers_o.fetch
  -- Pure C Parquet implementation
  let parquetWrapperObj ← lean_parquet_wrapper_o.fetch
  let parquetReaderWriterObj ← parquet_reader_writer_o.fetch
  let parquetWriterImplObj ← parquet_writer_impl_o.fetch
  let parquetReaderImplObj ← parquet_reader_impl_o.fetch
  -- IPC serialization (pure C)
  let ipcObj ← arrow_ipc_o.fetch
  let ipcWrapperObj ← lean_arrow_ipc_o.fetch
  -- Typed builders for direct Arrow array construction (pure C)
  let buildersObj ← arrow_builders_o.fetch
  let builderWrapperObj ← lean_builder_wrapper_o.fetch
  -- Nested type builders (List, Struct, Decimal128, Dictionary, Map)
  let nestedBuildersObj ← arrow_nested_builders_o.fetch
  -- Arrow compute functions (arithmetic, comparisons, aggregations)
  let computeObj ← arrow_compute_o.fetch
  let computeWrapperObj ← lean_arrow_compute_o.fetch
  -- ChunkedArray and Table
  let chunkedObj ← arrow_chunked_o.fetch
  let chunkedWrapperObj ← lean_arrow_chunked_o.fetch
  -- CSV/Parquet stub (no C++ dependencies)
  let csvParquetStubObj ← csv_parquet_stub_o.fetch
  buildStaticLib (pkg.staticLibDir / nameToStaticLib "arrow_wrapper")
    #[schemaObj, arrayObj, streamObj, dataAccessObj, bufferObj, wrapperObj, finalizersObj,
      parquetWrapperObj, parquetReaderWriterObj, parquetWriterImplObj, parquetReaderImplObj,
      ipcObj, ipcWrapperObj, buildersObj, builderWrapperObj, nestedBuildersObj, computeObj, computeWrapperObj,
      chunkedObj, chunkedWrapperObj, csvParquetStubObj]

require Cli from git
  "https://github.com/leanprover/lean4-cli.git" @ "main"

require zlogLean from git
  "git@github.com:marcellop71/zlog-lean.git" @ "main"

lean_exe examples where
  root := `Examples.Main
