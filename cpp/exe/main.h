#ifndef MAIN_H_
#define MAIN_H_

#include "cli_args.h"
#include <arrow/api.h>
#include <map>
#include <memory>
#include <boost/filesystem.hpp>

class sstable_t;
struct cli_args;

class s3_connection
{
    bool m_ok;

  public:
    s3_connection();
    ~s3_connection();
    bool ok() const;
};

/**
 * @brief If any specific files (index/statistics/summary) are passed for
 * inspection, inspect them using the functions in inspect_files
 */
arrow::Status run_arguments(cli_args args);

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> convert_sstables(
    std::map<int, std::shared_ptr<sstable_t>> sstables);

/**
 * @brief Read the file paths from the local filesystem and store them into the
 * given map of sstables.
 *
 * @param p path to the directory containing the sstable files
 * @param sstables the map of sstable_t objects where the paths will be stored
 */
void get_file_paths(const boost::filesystem::path &dir_path, std::map<int, std::shared_ptr<sstable_t>> &sstables);

/**
 * @brief Get the file paths from an S3 URI
 *
 * @param uri the uri to the directory inside the S3 bucket containing the
 * SSTable files, e.g. s3://my-bucket/path/to/s3
 * @param sstables the map of sstable_t objects where the paths will be stored
 */
arrow::Result<std::map<int, std::shared_ptr<sstable_t>>> get_file_paths_from_s3(const std::string &uri);

/**
 * @brief Decodes information from the file name and stores the full path in the
 * corresponding sstable_t object in the given map
 *
 * @param full_path the full path to the sstable file
 * @param file_name the final filename, e.g. md-1-big-Index.db
 * @param sstables the map of sstable_t objects where the paths will be stored
 */
void add_file_to_sstables(const std::string &full_path, const std::string &file_name,
                          std::map<int, std::shared_ptr<sstable_t>> &sstables);

#endif
