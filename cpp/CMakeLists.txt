cmake_minimum_required(VERSION 3.10.2)
project(sstable_to_arrow VERSION 0.1.0)

option(INCLUDE_WHAT_YOU_USE "check include directives")
option(TIDY "use clang tidy")

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})

if(TIDY)
    find_program(CLANG_TIDY NAMES "clang-tidy" "clang-tidy-10" REQUIRED)
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY};-checks=*")
endif()

if(INCLUDE_WHAT_YOU_USE)
    set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE "/usr/local/bin/include-what-you-use")
endif()

include(CPack)

find_program(KSC "kaitai-struct-compiler" REQUIRED)

find_package(Arrow REQUIRED)
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS filesystem iostreams python)

set(OPAQUE_TYPES deletion_time sstable_data sstable_index sstable_statistics sstable_summary sstable_compression_info)

add_executable(sstable_to_arrow
    opaque/clustering_blocks.cpp opaque/clustering_blocks.h
    opaque/columns_bitmask.cpp opaque/columns_bitmask.h
    opaque/deserialization_helper.cpp opaque/deserialization_helper.h
    opaque/modified_utf8.cpp opaque/modified_utf8.h
    opaque/vint.cpp opaque/vint.h
    ${PROJECT_BINARY_DIR}/deletion_time.cpp ${PROJECT_BINARY_DIR}/deletion_time.h
    ${PROJECT_BINARY_DIR}/sstable_data.cpp ${PROJECT_BINARY_DIR}/sstable_data.h
    ${PROJECT_BINARY_DIR}/sstable_index.cpp ${PROJECT_BINARY_DIR}/sstable_index.h
    ${PROJECT_BINARY_DIR}/sstable_statistics.cpp ${PROJECT_BINARY_DIR}/sstable_statistics.h
    ${PROJECT_BINARY_DIR}/sstable_summary.cpp ${PROJECT_BINARY_DIR}/sstable_summary.h
    ${PROJECT_BINARY_DIR}/sstable_compression_info.cpp ${PROJECT_BINARY_DIR}/sstable_compression_info.h
    util/sstable_to_arrow.cpp util/sstable_to_arrow.h
    util/opts.cpp util/opts.h # see if we can find better debugging options
    util/conversions.cpp util/conversions.h
    util/inspect_files.cpp util/inspect_files.h
    util/conversion_helper.cpp util/conversion_helper.h
    util/io.cpp util/io.h
    util/sstable.cpp util/sstable.h
    main.cpp main.h
    )

target_link_libraries(sstable_to_arrow PRIVATE
    kaitai_struct_cpp_stl_runtime
    arrow_shared
    ${Boost_LIBRARIES}
    Threads::Threads
    parquet
    lz4)

target_include_directories(sstable_to_arrow PUBLIC
    "${PROJECT_BINARY_DIR}"
    "${CMAKE_SOURCE_DIR}/util"
    "${CMAKE_SOURCE_DIR}/opaque")

if(MSVC)
    target_compile_options(sstable_to_arrow PRIVATE /W4 /WX)
else()
    target_compile_options(sstable_to_arrow PRIVATE -Wall -Wextra -pedantic)
endif()

foreach(ksy_file ${OPAQUE_TYPES})
    add_custom_command(
        OUTPUT "${PROJECT_BINARY_DIR}/${ksy_file}.h" "${PROJECT_BINARY_DIR}/${ksy_file}.cpp"
        COMMAND ${KSC} --target cpp_stl --cpp-standard 11 --opaque-types true --outdir "${PROJECT_BINARY_DIR}" "${CMAKE_SOURCE_DIR}/ksy/${ksy_file}.ksy"
        DEPENDS ${CMAKE_SOURCE_DIR}/ksy/${ksy_file}.ksy
    )
endforeach()
