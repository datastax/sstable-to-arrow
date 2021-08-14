include_guard()

cmake_minimum_required(VERSION 3.16)
project(
  SSTableParser
  VERSION 0.1.0
  DESCRIPTION "Parse SSTables into Kaitai Struct objects.")
set(LIBRARY_NAME sstable_parser)

# only depends on kaitai-struct-compiler
find_program(KSC "kaitai-struct-compiler" REQUIRED)
find_library(kaitai_struct_cpp_stl_runtime REQUIRED)

# compile kaitai files to source files in C++
set(KSY_TYPES deletion_time sstable_data sstable_index sstable_statistics
              sstable_summary sstable_compression_info)
foreach(KSY_FILE ${KSY_TYPES})
  add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/ksy/${KSY_FILE}.h"
           "${CMAKE_CURRENT_BINARY_DIR}/ksy/${KSY_FILE}.cpp"
    COMMAND
      ${KSC} --target cpp_stl --cpp-standard 11 --opaque-types true --outdir
      "${CMAKE_CURRENT_BINARY_DIR}/ksy"
      "${CMAKE_CURRENT_SOURCE_DIR}/ksy/${KSY_FILE}.ksy"
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ksy/${KSY_FILE}.ksy)
endforeach()

# transform kaitai filepaths to get path to each source file
list(TRANSFORM KSY_TYPES PREPEND ${CMAKE_CURRENT_BINARY_DIR}/ksy/)
list(TRANSFORM KSY_TYPES APPEND ".cpp")

# include all opaque types
set(OPAQUE_TYPES
    clustering_blocks.cpp columns_bitmask.cpp deserialization_helper.cpp
    modified_utf8.cpp vint.cpp conversions.cpp)
list(TRANSFORM OPAQUE_TYPES PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/opaque/)

# create the sstable_parser target from the source files
add_library(${LIBRARY_NAME} ${OPAQUE_TYPES} ${KSY_TYPES})

# dependents need to pass a kaitai::kstream, so we make this public
target_link_libraries(${LIBRARY_NAME} PUBLIC kaitai_struct_cpp_stl_runtime)

target_include_directories(
  ${LIBRARY_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/opaque>
         $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/ksy>
         $<INSTALL_INTERFACE:include/${LIBRARY_NAME}>)

target_compile_features(${LIBRARY_NAME} PUBLIC cxx_std_17)

# ===== CONFIGURE INSTALLATION =====

set(INSTALL_DEST lib/cmake/${PROJECT_NAME})
set(CONFIG_DEST ${PROJECT_NAME}Config)
set(VERSION_DEST ${CONFIG_DEST}Version)

# generate and install cmake file to import sstable_parser target
install(
  TARGETS ${LIBRARY_NAME}
  DESTINATION lib
  EXPORT ${PROJECT_NAME})
install(
  DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/opaque/ ${CMAKE_CURRENT_BINARY_DIR}/ksy/
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
