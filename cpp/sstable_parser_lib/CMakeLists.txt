find_program(KSC "kaitai-struct-compiler" REQUIRED)

set(KSY_TYPES deletion_time sstable_data sstable_index sstable_statistics
              sstable_summary sstable_compression_info)
foreach(KSY_FILE ${KSY_TYPES})
  add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/ksy/${KSY_FILE}.h"
           "${CMAKE_CURRENT_BINARY_DIR}/ksy/${KSY_FILE}.cpp"
    COMMAND
      ${KSC} --target cpp_stl --cpp-standard 11 --opaque-types true --outdir
      "${CMAKE_CURRENT_BINARY_DIR}/ksy"
      "${CMAKE_CURRENT_SOURCE_DIR}/ksy/${KSY_FILE}.ksy"
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ksy/${KSY_FILE}.ksy)
endforeach()

# transform kaitai types array to get path to each source file
list(TRANSFORM KSY_TYPES PREPEND ${CMAKE_CURRENT_BINARY_DIR}/ksy/)
list(TRANSFORM KSY_TYPES APPEND ".cpp")

# include all opaque types
set(OPAQUE_TYPES
    clustering_blocks.cpp columns_bitmask.cpp deserialization_helper.cpp
    modified_utf8.cpp vint.cpp conversions.cpp)
list(TRANSFORM OPAQUE_TYPES PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/opaque/)
add_library(sstable_parser ${OPAQUE_TYPES} ${KSY_TYPES})

target_include_directories(
  sstable_parser PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/opaque
                        ${CMAKE_CURRENT_BINARY_DIR}/ksy)
