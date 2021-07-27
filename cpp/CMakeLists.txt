cmake_minimum_required(VERSION 3.10.2)
project(sstable_to_arrow VERSION 0.1.0)

# include(CTest)
# enable_testing()

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)

find_package(Arrow REQUIRED)
find_package(Threads REQUIRED)
find_package(Boost REQUIRED filesystem)

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
    util/conversion_helper.cpp util/conversion_helper.h
    util/io.cpp util/io.h
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

option(PROFILE_GPROF "enable the -pg flag when linking to enable profiling with the gprof tool" OFF)
if(PROFILE_GPROF)
    target_link_libraries(sstable_to_arrow PRIVATE "-pg")
endif(PROFILE_GPROF)

option(DEBUG_VERBOSE "print verbose messages detailing code" OFF)
if(DEBUG_VERBOSE)
    add_definitions(-DDEBUG)
endif(DEBUG_VERBOSE)

foreach(ksy_file deletion_time sstable_data sstable_index sstable_statistics sstable_summary sstable_compression_info)
    add_custom_command(
        OUTPUT "${PROJECT_BINARY_DIR}/${ksy_file}.h" "${PROJECT_BINARY_DIR}/${ksy_file}.cpp"
        COMMAND /usr/bin/kaitai-struct-compiler --target cpp_stl --cpp-standard 11 --opaque-types true --outdir "${PROJECT_BINARY_DIR}" "${CMAKE_SOURCE_DIR}/ksy/${ksy_file}.ksy"
        DEPENDS /usr/bin/kaitai-struct-compiler ${CMAKE_SOURCE_DIR}/ksy/${ksy_file}.ksy
    )
endforeach()
