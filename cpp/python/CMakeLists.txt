cmake_minimum_required(VERSION 3.16)
project(testing)

find_package(Arrow REQUIRED)
find_package(Boost REQUIRED COMPONENTS python3)
find_package(Python3 REQUIRED COMPONENTS Development NumPy)

add_library(testing SHARED testing.cpp)

set_target_properties(testing PROPERTIES PREFIX "")
target_link_libraries(testing PRIVATE
    arrow_shared
    arrow_python
    ${Boost_LIBRARIES}
    Python3::NumPy
    Python3::Python
    )

install(TARGETS testing DESTINATION lib)
