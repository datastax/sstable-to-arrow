cmake_minimum_required(VERSION 3.16)
project(sstable_parser VERSION 0.1.0)

# only depends on kaitai-struct-compiler
find_program(KSC "kaitai-struct-compiler" REQUIRED)
find_library(kaitai_struct_cpp_stl_runtime REQUIRED)

# compile kaitai files to source files in C++
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

# transform kaitai filepaths to get path to each source file
list(TRANSFORM KSY_TYPES PREPEND ${CMAKE_CURRENT_BINARY_DIR}/ksy/)
list(TRANSFORM KSY_TYPES APPEND ".cpp")

# include all opaque types
set(OPAQUE_TYPES
    clustering_blocks.cpp columns_bitmask.cpp deserialization_helper.cpp
    modified_utf8.cpp vint.cpp conversions.cpp)
list(TRANSFORM OPAQUE_TYPES PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/opaque/)

# create the sstable_parser target from the source files
add_library(${PROJECT_NAME} SHARED ${OPAQUE_TYPES} ${KSY_TYPES})

target_link_libraries(${PROJECT_NAME} PRIVATE kaitai_struct_cpp_stl_runtime)

target_include_directories(
  ${PROJECT_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/opaque
                         ${CMAKE_CURRENT_BINARY_DIR}/ksy)

set_target_properties(${PROJECT_NAME} PROPERTIES CXX_STANDARD 17
                                                 CXX_STANDARD_REQUIRED ON)

install(TARGETS ${PROJECT_NAME} DESTINATION lib)
install(
  DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/opaque/ ${CMAKE_CURRENT_BINARY_DIR}/ksy/
  DESTINATION include/${PROJECT_NAME}
  FILES_MATCHING
  PATTERN "*.h")
