cmake_minimum_required(VERSION 3.12)
project(sstable_to_arrow VERSION 0.1.0)

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

# options for printing out messages for debugging
option(INCLUDE_WHAT_YOU_USE "check include directives")
option(TIDY "use clang tidy")

if(TIDY)
  find_program(CLANG_TIDY NAMES "clang-tidy" "clang-tidy-10" REQUIRED)
  set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY};-checks=*")
endif()

if(INCLUDE_WHAT_YOU_USE)
  set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE "/usr/local/bin/include-what-you-use")
endif()

add_subdirectory(sstable_parser_lib)

find_package(Arrow REQUIRED)
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS filesystem iostreams python)

add_executable(
  sstable_to_arrow
  util/sstable_to_arrow.cpp
  util/sstable_to_arrow.h
  util/opts.cpp
  util/opts.h # see if we can find better debugging options
  util/inspect_files.cpp
  util/inspect_files.h
  util/conversion_helper.cpp
  util/conversion_helper.h
  util/io.cpp
  util/io.h
  util/sstable.cpp
  util/sstable.h
  main.cpp
  main.h)

target_link_libraries(
  sstable_to_arrow
  PRIVATE arrow_shared
          ${Boost_LIBRARIES}
          kaitai_struct_cpp_stl_runtime
          lz4
          parquet
          sstable_parser
          Threads::Threads)

target_include_directories(sstable_to_arrow
                           PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/util")

if(MSVC)
  target_compile_options(sstable_to_arrow PRIVATE /W4 /WX)
else()
  target_compile_options(sstable_to_arrow PRIVATE -Wall -Wextra -pedantic)
endif()

# use CPack to create platform specific installers
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE.txt")
set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)
