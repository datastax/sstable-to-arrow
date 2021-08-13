cmake_minimum_required(VERSION 3.16)
project(sstable_to_pyarrow)

find_package(Arrow REQUIRED)
find_package(Boost REQUIRED COMPONENTS python3)
find_package(Python3 REQUIRED COMPONENTS Development NumPy)

add_library(sstable_to_pyarrow SHARED sstable_to_pyarrow.cpp)

set_target_properties(sstable_to_pyarrow PROPERTIES PREFIX "")
target_link_libraries(
  sstable_to_pyarrow PRIVATE arrow_shared arrow_python ${Boost_LIBRARIES} Python3::NumPy
                  Python3::Python)

install(TARGETS sstable_to_pyarrow DESTINATION lib)
