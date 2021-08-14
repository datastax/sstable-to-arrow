include_guard()

cmake_minimum_required(VERSION 3.16)
project(
  SSTableToArrowExecutable
  VERSION 0.1.0
  DESCRIPTION
    "Command line executable for converting SSTables written by Cassandra into Arrow Tables"
)
set(SSTableToArrowExecutable_TARGET_NAME sstable_to_arrow_exe)

add_subdirectory(../src sstable_to_arrow)

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)

# find required packages to link to
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS filesystem iostreams python)
find_package(Arrow REQUIRED)
find_library(kaitai_struct_cpp_stl_runtime REQUIRED)

# create main sstable_to_arrow executable target
add_executable(${SSTableToArrowExecutable_TARGET_NAME} main.cpp
                                                       cli_args.cpp cli_args.h)

target_link_libraries(
  ${SSTableToArrowExecutable_TARGET_NAME}
  PRIVATE arrow_shared ${Boost_LIBRARIES} kaitai_struct_cpp_stl_runtime
          sstable_to_arrow sstable_parser Threads::Threads)

target_include_directories(${SSTableToArrowExecutable_TARGET_NAME}
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if(MSVC)
  target_compile_options(${SSTableToArrowExecutable_TARGET_NAME} PRIVATE /W4
                                                                         /WX)
else()
  target_compile_options(${SSTableToArrowExecutable_TARGET_NAME}
                         PRIVATE -Wall -Wextra -pedantic)
endif()

set_target_properties(
  ${SSTableToArrowExecutable_TARGET_NAME}
  PROPERTIES OUTPUT_NAME "sstable-to-arrow"
             CXX_STANDARD 17
             CXX_STANDARD_REQUIRED ON)
