cmake_minimum_required(VERSION 3.0.0)
project(sstable_to_arrow VERSION 0.1.0)

include(CTest)
enable_testing()

find_package(Arrow REQUIRED)

add_executable(sstable_to_arrow
    src/util/clustering_blocks.cpp src/util/clustering_blocks.h
    src/util/columns_bitmask.cpp src/util/columns_bitmask.h
    src/util/deserialization_helper.cpp src/util/deserialization_helper.h
    src/util/modified_utf8.cpp src/util/modified_utf8.h
    src/util/vint.cpp src/util/vint.h
    ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/deletion_time.h ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/deletion_time.cpp
    ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_data.h ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_data.cpp
    ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_index.h ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_index.cpp
    ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_statistics.h ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_statistics.cpp
    ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_summary.h ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_summary.cpp
    src/main.cpp src/main.h
    src/sstable_to_arrow.cpp src/sstable_to_arrow.h
    )

target_link_libraries(sstable_to_arrow PRIVATE kaitai_struct_cpp_stl_runtime)
target_link_libraries(sstable_to_arrow PRIVATE arrow_shared)

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

target_include_directories(sstable_to_arrow PUBLIC "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}")
target_include_directories(sstable_to_arrow PUBLIC "${CMAKE_SOURCE_DIR}/src/util")

MESSAGE(STATUS "CMAKE_CURRENT_BINARY_DIR: " ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY})

add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/deletion_time.h" "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/deletion_time.cpp"
    COMMAND echo "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}" && pwd && /usr/bin/kaitai-struct-compiler --target cpp_stl --cpp-standard 11 --opaque-types true --outdir "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}" "${CMAKE_SOURCE_DIR}/src/ksy/deletion_time.ksy"
    DEPENDS /usr/bin/kaitai-struct-compiler
)

add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_data.h" "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_data.cpp"
    COMMAND /usr/bin/kaitai-struct-compiler --target cpp_stl --cpp-standard 11 --opaque-types true --outdir "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}" "${CMAKE_SOURCE_DIR}/src/ksy/sstable_data.ksy"
    DEPENDS /usr/bin/kaitai-struct-compiler
)

add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_index.h" "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_index.cpp"
    COMMAND /usr/bin/kaitai-struct-compiler --target cpp_stl --cpp-standard 11 --opaque-types true --outdir "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}" "${CMAKE_SOURCE_DIR}/src/ksy/sstable_index.ksy"
    DEPENDS /usr/bin/kaitai-struct-compiler
)

add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_statistics.h" "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_statistics.cpp"
    COMMAND /usr/bin/kaitai-struct-compiler --target cpp_stl --cpp-standard 11 --opaque-types true --outdir "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}" "${CMAKE_SOURCE_DIR}/src/ksy/sstable_statistics.ksy"
    DEPENDS /usr/bin/kaitai-struct-compiler
)

add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_summary.h" "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/sstable_summary.cpp"
    COMMAND /usr/bin/kaitai-struct-compiler --target cpp_stl --cpp-standard 11 --opaque-types true --outdir "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}" "${CMAKE_SOURCE_DIR}/src/ksy/sstable_summary.ksy"
    DEPENDS /usr/bin/kaitai-struct-compiler
)
