cmake_minimum_required(VERSION 3.16)
project(SSTableToPyArrow)
set(MODULE_NAME sstable_to_pyarrow)

find_package(Arrow REQUIRED)
find_package(Boost REQUIRED COMPONENTS python3)
find_package(Python3 REQUIRED COMPONENTS Development NumPy)
find_package(SSTableToArrow REQUIRED)
# TODO find out how to get this into SSTableToArrow package
find_package(SSTableParser REQUIRED)

configure_file(sstable_to_pyarrow.h.in sstable_to_pyarrow.h)

add_library(${MODULE_NAME} SHARED sstable_to_pyarrow.cpp)

set_target_properties(
  ${MODULE_NAME}
  PROPERTIES PREFIX ""
             CXX_STANDARD 17
             CXX_STANDARD_REQUIRED ON)
target_link_libraries(
  ${MODULE_NAME}
  PRIVATE arrow_shared
          arrow_python
          ${Boost_LIBRARIES}
          Python3::NumPy
          Python3::Python
          sstable_parser
          sstable_to_arrow)
target_include_directories(${MODULE_NAME} PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
install(TARGETS ${MODULE_NAME} DESTINATION lib)
