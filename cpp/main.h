#ifndef MAIN_H_
#define MAIN_H_

#include <stdint.h>
#include <getopt.h>
#include <unistd.h>
#include <dirent.h>
#include <memory>
#include <fstream>
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <map>
#include <lz4.h>
#include <stdexcept>

#include "sstable_statistics.h"
#include "sstable_data.h"
#include "sstable_index.h"
#include "sstable_summary.h"
#include "inspect_files.h"
#include "sstable_compression_info.h"
#include "sstable_to_arrow.h"
#include "deserialization_helper.h"
#include "io.h"
#include "opts.h"

class s3_connection
{
    bool m_ok;

public:
    s3_connection();
    ~s3_connection();
    bool ok() const;
};

struct sstable_t
{
    boost::filesystem::path statistics_path, data_path, index_path, summary_path, compression_info_path;

    std::shared_ptr<sstable_statistics_t> statistics;
    std::shared_ptr<sstable_data_t> data;
    std::shared_ptr<sstable_index_t> index;
    std::shared_ptr<sstable_summary_t> summary;
    std::shared_ptr<sstable_compression_info_t> compression_info;
};

/**
 * @brief If any specific files (index/statistics/summary) are passed for inspection,
 * inspect them using the functions in inspect_files
 */
arrow::Status run_arguments();

/**
 * @brief Takes the paths stored in the given sstable object and loads the actual data using kaitai
 */
arrow::Status load_sstable_files(std::shared_ptr<sstable_t> sstable);

arrow::Status read_data(std::shared_ptr<sstable_t> sstable);

/**
 * @brief Takes the compression information and a path to the compressed data file and returns a parsed sstable_data_t object.
 * 
 * @param compression_info An object containing the information stored in CompressionInfo.db
 * @param data_path The path to the compressed data file
 * @return the decompressed data object
 */
arrow::Result<std::shared_ptr<sstable_data_t>> read_decompressed_sstable(const std::shared_ptr<sstable_compression_info_t> &compression_info, const boost::filesystem::path &data_path);

/**
 * @brief Reads the file with the format given by the kaitai::kstruct type T.
 * 
 * @tparam T e.g. sstable_t, sstable_statistics_t, etc
 * @param path The path to the file or the S3 URI of the file
 */
template <typename T>
arrow::Result<std::shared_ptr<T>> read_sstable_file(const boost::filesystem::path &path);

template <>
arrow::Result<std::shared_ptr<sstable_statistics_t>> read_sstable_file(const boost::filesystem::path &path);

/**
 * @brief Read the file paths from the local filesystem and store them into the given map of sstables.
 * 
 * @param p path to the directory containing the sstable files
 * @param sstables the map of sstable_t objects where the paths will be stored
 */
void get_file_paths(const boost::filesystem::path &dir_path, std::map<int, std::shared_ptr<sstable_t>> &sstables);

/**
 * @brief Get the file paths from an S3 URI
 * 
 * @param uri the uri to the directory inside the S3 bucket containing the SSTable files, e.g. s3://my-bucket/path/to/s3
 * @param sstables the map of sstable_t objects where the paths will be stored
 */
arrow::Status get_file_paths_from_s3(const std::string &uri, std::map<int, std::shared_ptr<sstable_t>> &sstables);

/**
 * @brief Decodes information from the file name and stores the full path in the corresponding sstable_t object in the given map
 * 
 * @param full_path the full path to the sstable file
 * @param file_name the final filename, e.g. md-1-big-Index.db
 * @param sstables the map of sstable_t objects where the paths will be stored
 */
void add_file_to_sstables(const std::string &full_path, const std::string &file_name, std::map<int, std::shared_ptr<sstable_t>> &sstables);

/**
 * @brief Opens an input stream to the file specified by `path` and stores it in the object pointed to by `ifs`.
 * 
 * @param path the file to open an input stream to
 * @param ifs where to store the resulting input stream
 */
arrow::Result<std::shared_ptr<std::istream>> open_stream(const boost::filesystem::path &path);

/**
 * @brief Initialize the deserialization_header for parsing the Data.db file
 * 
 * @param serialization_header the serialization header containing the schema information for the sstable
 */
void init_deserialization_helper(sstable_statistics_t::serialization_header_t *serialization_header);

#endif
