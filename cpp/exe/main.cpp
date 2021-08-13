#include "main.h"
#include "inspect_files.h"
#include "io.h"
#include "sstable.h"
#include "sstable_to_arrow.h"
#include <arrow/filesystem/api.h>
#include <boost/algorithm/string.hpp>
#include <iostream>

namespace arrow
{
class Table;
} // namespace arrow

#define EXIT_NOT_OK(expr, msg)                                                                                         \
    do                                                                                                                 \
    {                                                                                                                  \
        const arrow::Status &_s = (expr);                                                                              \
        if (!_s.ok())                                                                                                  \
        {                                                                                                              \
            std::cerr << (msg) << ": " << _s.message() << '\n';                                                        \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

const int MAX_FILEPATH_SIZE = 45;

int main(int argc, char *argv[])
{
    const cli_args &args = read_options(argc, argv);

    std::shared_ptr<arrow::fs::S3FileSystem> s3fs;

    if (args.show_help)
    {
        std::cout << help_msg << '\n';
        return 0;
    }

    if (!args.errors.empty())
    {
        std::cerr << "invalid arguments:\n";
        for (const std::string &err : args.errors)
        {
            std::cerr << err << '\n';
        }
        return 1;
    }

    EXIT_NOT_OK(run_arguments(args), "error running arguments");

    if (!args.read_sstable_dir)
    {
        return 0;
    }

    std::vector<std::shared_ptr<arrow::Table>> finished_tables;
    if (args.is_s3)
    {
        // establish connection to S3 with RAII
        s3_connection conn;
        if (!conn.ok())
        {
            std::cerr << "error connecting to S3\n";
            return 1;
        }

        auto result = get_file_paths_from_s3(args.sstable_dir_path);
        EXIT_NOT_OK(result.status(), "error loading from S3");
        auto sstables_result = convert_sstables(result.ValueOrDie());
        EXIT_NOT_OK(sstables_result.status(), "error converting sstables");
        finished_tables = std::move(sstables_result.ValueOrDie());
    }
    else
    {
        std::map<int, std::shared_ptr<sstable_t>> sstables;
        get_file_paths(args.sstable_dir_path, sstables);
        auto sstables_result = convert_sstables(sstables);
        EXIT_NOT_OK(sstables_result.status(), "error converting sstables");
        finished_tables = std::move(sstables_result.ValueOrDie());
    }

    if (args.write_parquet)
        EXIT_NOT_OK(write_parquet(args.parquet_dst_path, finished_tables), "error writing parquet");

    if (args.listen)
        EXIT_NOT_OK(send_tables(finished_tables), "error sending tables");
    else
        for (auto &entry : finished_tables)
            std::cout << entry->ToString() << '\n';

    return 0;
}

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> convert_sstables(
    std::map<int, std::shared_ptr<sstable_t>> sstables)
{
    if (sstables.empty())
        return arrow::Status::Invalid("no sstables found");

    std::vector<std::shared_ptr<arrow::Table>> finished_tables(sstables.size());

    int i = 0;
    for (auto &entry : sstables)
    {
        std::cout << "\n\n========== Reading SSTable #" << entry.first << " ==========\n";
        ARROW_RETURN_NOT_OK(entry.second->init());

        ARROW_ASSIGN_OR_RAISE(finished_tables[i++],
                              vector_to_columnar_table(entry.second->statistics(), entry.second->data()));
    }

    return finished_tables;
}

arrow::Status run_arguments(cli_args args)
{
    if (args.statistics_only)
    {
        file_container<sstable_statistics_t> statistics;
        statistics.set_path(args.statistics_path);
        ARROW_RETURN_NOT_OK(statistics.init());
        debug_statistics(statistics.file());
    }
    else if (args.index_only)
    {
        file_container<sstable_index_t> index;
        index.set_path(args.index_path);
        ARROW_RETURN_NOT_OK(index.init());
        debug_index(index.file());
    }
    else if (args.summary_only)
    {
        file_container<sstable_summary_t> summary;
        summary.set_path(args.summary_path);
        ARROW_RETURN_NOT_OK(summary.init());
        debug_summary(summary.file());
    }
    return arrow::Status::OK();
}

s3_connection::s3_connection()
{
    std::cout << "opening connection to s3\n";
    m_ok = arrow::fs::EnsureS3Initialized().ok();
}

s3_connection::~s3_connection()
{
    std::cout << "closing connection to s3\n";
    m_ok = arrow::fs::FinalizeS3().ok();
}

bool s3_connection::ok() const
{
    return m_ok;
}

/**
 * @brief Get the file paths of each SSTable file in a folder
 * Iterates through the specified folder and reads the file names into sstable
 * structs that map the file type (e.g. statistics, data) to the file
 * path.
 * @param path the path to the folder containing the SSTable files
 */
void get_file_paths(const boost::filesystem::path &dir_path, std::map<int, std::shared_ptr<sstable_t>> &sstables)
{
    namespace fs = boost::filesystem;
    for (const fs::directory_entry &file : fs::directory_iterator(dir_path))
        if (fs::is_regular_file(file.path()))
            add_file_to_sstables(file.path().string(), file.path().filename().string(), sstables);
}

arrow::Result<std::map<int, std::shared_ptr<sstable_t>>> get_file_paths_from_s3(const std::string &uri)
{
    std::map<int, std::shared_ptr<sstable_t>> sstables;

    // get the bucket uri and the actual path to the file
    size_t pos = uri.find('/', 5);
    std::string bucket_uri{pos == std::string::npos ? uri : uri.substr(0, pos)};
    std::string path = uri.substr(5);

    ARROW_RETURN_NOT_OK(arrow::fs::EnsureS3Initialized());

    // create the S3 filesystem
    ARROW_ASSIGN_OR_RAISE(auto options, arrow::fs::S3Options::FromUri(bucket_uri));
    ARROW_ASSIGN_OR_RAISE(global_flags.s3fs, arrow::fs::S3FileSystem::Make(options));

    // get the list of files in the directory pointed to by `path`
    arrow::fs::FileSelector selector;
    selector.base_dir = path;
    ARROW_ASSIGN_OR_RAISE(auto file_info, global_flags.s3fs->GetFileInfo(selector));

    for (auto &info : file_info)
    {
        if (info.IsFile())
        {
            add_file_to_sstables(info.path(), info.base_name(), sstables);
        }
    }

    return sstables;
}

void add_file_to_sstables(const std::string &full_path, const std::string &file_name,
                          std::map<int, std::shared_ptr<sstable_t>> &sstables)
{
    char fmt[5];
    int num;
    char db_type_buf[20]; // longest sstable file type specifier is
                          // "CompressionInfo"

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
