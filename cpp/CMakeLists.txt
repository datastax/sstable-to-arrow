cmake_minimum_required(VERSION 3.16)
project(sstable_to_arrow VERSION 0.1.0)

# find required packages to link to
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS filesystem iostreams python)

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

# add subdirectories
add_subdirectory(parser)
add_subdirectory(conversion_lib)

# create main sstable_to_arrow executable target
add_executable(sstable_to_arrow main.cpp main.h)

target_link_libraries(
  sstable_to_arrow PRIVATE sstable_to_arrow_lib sstable_parser
                           ${Boost_LIBRARIES} Threads::Threads)

if(MSVC)
  target_compile_options(sstable_to_arrow PRIVATE /W4 /WX)
else()
  target_compile_options(sstable_to_arrow PRIVATE -Wall -Wextra -pedantic)
endif()

include(test/sstable_to_arrow.cmake)

# use CPack to create platform specific installers
include(InstallRequiredSystemLibraries)
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE.txt")
set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
include(CPack)
