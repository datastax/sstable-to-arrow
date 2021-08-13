cmake_minimum_required(VERSION 3.16)
project(sstable_to_arrow_lib VERSION 0.1.0)

# find required components
find_package(Arrow REQUIRED)
find_library(kaitai_struct_cpp_stl_runtime REQUIRED)
find_library(lz4 REQUIRED)
find_library(parquet REQUIRED)
find_library(sstable_parser REQUIED)
find_package(Threads REQUIRED)

# create sstable_to_arrow_lib target
add_library(sstable_to_arrow_lib conversion_helper.cpp inspect_files.cpp io.cpp
                                 opts.cpp sstable_to_arrow.cpp sstable.cpp)

# TODO is there a better way to propagate the include directories from
# sstable_parser and keep the library itself private?
target_link_libraries(
  sstable_to_arrow_lib
  PRIVATE arrow_shared
          ${Boost_LIBRARIES}
          kaitai_struct_cpp_stl_runtime
          lz4
          parquet
          Threads::Threads
          sstable_parser)

target_include_directories(
  sstable_to_arrow_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
                              ${sstable_parser_INCLUDE_DIRS})
