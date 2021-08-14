#ifndef OPTS_H_
#define OPTS_H_

#include <arrow/result.h> // for Result
#include <iostream>       // for cout
#include <map>            // for map
#include <memory>         // for shared_ptr
#include <string>         // for string
#include <string_view>    // for string_view
namespace arrow { namespace fs { class S3FileSystem; } }
namespace sstable_to_arrow { class sstable_t; }

namespace sstable_to_arrow
{

namespace
{

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

} // namespace

struct flags
{
    bool include_metadata = true;
    bool for_cudf = false;
    bool is_s3 = false;
    bool verbose = false;

    std::shared_ptr<arrow::fs::S3FileSystem> s3fs;
};

extern flags global_flags;

/**
 * @brief Read the file paths from the local filesystem and store them into the
 * given map of sstables.
 *
 * @param p path to the directory containing the sstable files
 * @param sstables the map of sstable_t objects where the paths will be stored
 */
std::map<int, std::shared_ptr<sstable_t>> get_file_paths_from_local(std::string_view path);

/**
 * @brief Get the file paths from an S3 URI
 *
 * @param uri the uri to the directory inside the S3 bucket containing the
 * SSTable files, e.g. s3://my-bucket/path/to/s3
 * @param sstables the map of sstable_t objects where the paths will be stored
 */
arrow::Result<std::map<int, std::shared_ptr<sstable_t>>> get_file_paths_from_s3(std::string_view uri);

} // namespace sstable_to_arrow

template <typename T> inline void DEBUG_ONLY(T msg)
{
    if (sstable_to_arrow::global_flags.verbose)
        std::cout << msg;
}

#endif
