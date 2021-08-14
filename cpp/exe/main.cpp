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
            std::cerr << err << '\n';
        return 1;
    }

    EXIT_NOT_OK(run_arguments(args), "error running arguments");

    if (!args.read_sstable_dir)
        return 0;

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
        auto sstables = read_sstables(args.sstable_dir_path);
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
