cmake_minimum_required(VERSION 3.16)
project(
  SSTableToArrow
  VERSION 0.1.0
  DESCRIPTION "Convert SSTable files to Arrow Tables")
set(LIBRARY_NAME sstable_to_arrow)

add_subdirectory(../parser ${CMAKE_CURRENT_BINARY_DIR}/sstable_parser)

# find required components
find_package(Arrow REQUIRED)
find_package(Boost REQUIRED COMPONENTS filesystem)
find_library(kaitai_struct_cpp_stl_runtime REQUIRED)
find_library(lz4 REQUIRED)
find_library(parquet REQUIRED)
find_package(Threads REQUIRED)

# create sstable_to_arrow target
add_library(
  ${LIBRARY_NAME} SHARED conversion_helper.cpp inspect_files.cpp io.cpp
                         opts.cpp sstable_to_arrow.cpp sstable.cpp)

# TODO is there a better way to propagate the include directories from
# sstable_parser and keep the library itself private?
target_link_libraries(
  ${LIBRARY_NAME}
  PRIVATE arrow_shared ${Boost_LIBRARIES} kaitai_struct_cpp_stl_runtime lz4
          parquet Threads::Threads
  PUBLIC sstable_parser)

target_include_directories(
  ${LIBRARY_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
                         $<INSTALL_INTERFACE:include/${LIBRARY_NAME}>)

set_target_properties(${LIBRARY_NAME} PROPERTIES CXX_STANDARD 17
                                                 CXX_STANDARD_REQUIRED ON)

# ===== INSTALL CONFIGURATION =====

set(INSTALL_DEST lib/cmake/${PROJECT_NAME})
set(CONFIG_DEST ${PROJECT_NAME}Config)
set(VERSION_DEST ${CONFIG_DEST}Version)

# generate and install cmake file to import sstable_parser target
install(
  TARGETS ${LIBRARY_NAME}
  DESTINATION lib
  EXPORT ${PROJECT_NAME})
install(
  DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
  DESTINATION include/${LIBRARY_NAME}
  FILES_MATCHING
  PATTERN "*.h")

# install the generated cmake file
install(
  EXPORT ${PROJECT_NAME}
  FILE ${PROJECT_NAME}.cmake
  DESTINATION ${INSTALL_DEST})

include(CMakePackageConfigHelpers)
# generate config file that includes the exports
configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/Config.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/${CONFIG_DEST}.cmake"
  INSTALL_DESTINATION "${INSTALL_DEST}"
  NO_SET_AND_CHECK_MACRO NO_CHECK_REQUIRED_COMPONENTS_MACRO)
# generate version file for config file
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/${VERSION_DEST}.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY AnyNewerVersion)
# install config file
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${CONFIG_DEST}.cmake
              ${CMAKE_CURRENT_BINARY_DIR}/${VERSION_DEST}.cmake
        DESTINATION ${INSTALL_DEST})
# allow project to be used from build directory
export(EXPORT ${PROJECT_NAME}
       FILE ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.cmake)
