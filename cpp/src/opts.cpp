#include "opts.h"
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

namespace sstable_to_arrow
{
namespace
{

void add_file_to_sstables(const std::string &full_path, const std::string &file_name,
                          std::map<int, std::shared_ptr<sstable_t>> &sstables)
{
    char fmt[5];
    int num;
    char db_type_buf[20]; // longest sstable file type specifier is
                          // "CompressionInfo"

    const int MAX_FILEPATH_SIZE = 45;

    if (!boost::iends_with(file_name, ".db") || (file_name.size() > MAX_FILEPATH_SIZE))
    {
        std::cout << "skipping unrecognized file " << file_name << '\n';
        return;
    }

    if (sscanf(file_name.data(), "%2c-%d-big-%19[^.]", fmt, &num, db_type_buf) != 3) // number of arguments filled
    {
        std::cout << "Error reading formatted filename " << file_name << ", skipping\n";
        return;
    }

    std::string_view db_type(db_type_buf);

    // create this sstable if it does not yet exist
    if (sstables.count(num) == 0)
        sstables[num] = std::make_shared<sstable_t>();

    if (db_type == "Data")
        sstables[num]->set_data_path(full_path);
    else if (db_type == "Statistics")
        sstables[num]->set_statistics_path(full_path);
    else if (db_type == "Index")
        sstables[num]->set_index_path(full_path);
    else if (db_type == "Summary")
        sstables[num]->set_summary_path(full_path);
    else if (db_type == "CompressionInfo")
        sstables[num]->set_compression_info_path(full_path);
    else
        std::cout << "skipping unrecognized file " << file_name << '\n';
}

} // namespace

flags global_flags;

/**
 * @brief Get the file paths of each SSTable file in a folder
 * Iterates through the specified folder and reads the file names into sstable
 * structs that map the file type (e.g. statistics, data) to the file
 * path.
 * @param path the path to the folder containing the SSTable files
 */
std::map<int, std::shared_ptr<sstable_t>> get_file_paths_from_local(std::string_view path)
{
    namespace fs = boost::filesystem;
    std::map<int, std::shared_ptr<sstable_t>> sstables;
    for (const fs::directory_entry &file : fs::directory_iterator(fs::path{path.data()}))
        if (fs::is_regular_file(file.path()))
            add_file_to_sstables(file.path().string(), file.path().filename().string(), sstables);
    return sstables;
}

arrow::Result<std::map<int, std::shared_ptr<sstable_t>>> get_file_paths_from_s3(std::string_view uri)
{
    std::map<int, std::shared_ptr<sstable_t>> sstables;

    // get the bucket uri and the actual path to the file
    size_t pos = uri.find('/', 5);
    std::string bucket_uri{pos == std::string::npos ? uri : uri.substr(0, pos)};
    std::string_view path = uri.substr(5);

    ARROW_RETURN_NOT_OK(arrow::fs::EnsureS3Initialized());

    // create the S3 filesystem
    ARROW_ASSIGN_OR_RAISE(auto options, arrow::fs::S3Options::FromUri(bucket_uri));
    ARROW_ASSIGN_OR_RAISE(global_flags.s3fs, arrow::fs::S3FileSystem::Make(options));

    // get the list of files in the directory pointed to by `path`
    arrow::fs::FileSelector selector;
    selector.base_dir = path;
    ARROW_ASSIGN_OR_RAISE(auto file_info, global_flags.s3fs->GetFileInfo(selector));

    for (auto &info : file_info)
        if (info.IsFile())
            add_file_to_sstables(info.path(), info.base_name(), sstables);

    return sstables;
}

} // namespace sstable_to_arrow
