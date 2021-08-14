cmake_minimum_required(VERSION 3.16)
project(sstable_to_arrow VERSION 0.1.0)

# exe includes both of the other two directories
# this is probably not the right way to do it but it works
add_subdirectory(exe)

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

include(test/sstable_to_arrow.cmake)

install(TARGETS sstable_to_arrow_exe DESTINATION bin)

# use CPack to create platform specific installers
include(InstallRequiredSystemLibraries)
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE.txt")
set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
include(CPack)
