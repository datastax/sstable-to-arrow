include_guard()

cmake_minimum_required(VERSION 3.16)
project(SSTableToPyArrow)
# this is both the name of the target and the name of the python module
set(MODULE_NAME sstable_to_pyarrow)

add_subdirectory(../src sstable_to_arrow)

find_package(Arrow REQUIRED)
find_package(Boost REQUIRED COMPONENTS python3 filesystem)
find_package(Python3 REQUIRED COMPONENTS Development NumPy)

# has to be a shared library for python to find it
add_library(${MODULE_NAME} SHARED sstable_to_pyarrow.cpp)

set_target_properties(
  ${MODULE_NAME}
  PROPERTIES PREFIX ""
             CXX_STANDARD 17
             CXX_STANDARD_REQUIRED ON
             POSITION_INDEPENDENT_CODE ${BUILD_SHARED_LIBS})

target_link_libraries(
  ${MODULE_NAME}
  PRIVATE arrow_shared
          arrow_python
          ${Boost_LIBRARIES}
          Python3::NumPy
          Python3::Python
          sstable_parser
          sstable_to_arrow)

target_compile_definitions(${MODULE_NAME} PRIVATE MODULE_NAME=${MODULE_NAME})
target_include_directories(${MODULE_NAME} PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
install(TARGETS ${MODULE_NAME} DESTINATION lib)
