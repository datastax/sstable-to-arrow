cmake_minimum_required(VERSION 3.16)

add_subdirectory(../src ${CMAKE_CURRENT_BINARY_DIR}/sstable_to_arrow)

set(EXE_TARGET_NAME sstable_to_arrow_exe)
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)

option(INCLUDE_WHAT_YOU_USE "check include directives")
if(INCLUDE_WHAT_YOU_USE)
  find_program(IWYU "include-what-you-use" REQUIRED)
  set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU})
endif()

option(TIDY "use clang tidy")
if(TIDY)
  find_program(CLANG_TIDY NAMES "clang-tidy" "clang-tidy-10" REQUIRED)
  set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY};-checks=*")
endif()

# find required packages to link to
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS filesystem iostreams python)
find_package(Arrow REQUIRED)
find_library(kaitai_struct_cpp_stl_runtime REQUIRED)

# create main sstable_to_arrow executable target
add_executable(${EXE_TARGET_NAME} main.cpp main.h cli_args.cpp cli_args.h)

target_link_libraries(
  ${EXE_TARGET_NAME}
  PRIVATE arrow_shared ${Boost_LIBRARIES} kaitai_struct_cpp_stl_runtime
          sstable_to_arrow sstable_parser Threads::Threads)

target_include_directories(${EXE_TARGET_NAME}
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if(MSVC)
  target_compile_options(${EXE_TARGET_NAME} PRIVATE /W4 /WX)
else()
  target_compile_options(${EXE_TARGET_NAME} PRIVATE -Wall -Wextra -pedantic)
endif()

set_target_properties(
  ${EXE_TARGET_NAME}
  PROPERTIES OUTPUT_NAME "sstable-to-arrow"
             CXX_STANDARD 17
             CXX_STANDARD_REQUIRED ON)
