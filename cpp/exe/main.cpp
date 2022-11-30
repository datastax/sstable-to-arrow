#include "api.h"                // for read_sstables
#include "cli_args.h"           // for cli_args, read_options, help_msg
#include "inspect_files.h"      // for debug_index, debug_statistics, debug...
#include "io.h"                 // for send_tables, write_parquet
#include "sstable.h"            // for file_container
#include "sstable_index.h"      // for sstable_index_t
#include "sstable_statistics.h" // for sstable_statistics_t
#include "sstable_summary.h"    // for sstable_summary_t
#include <arrow/result.h>       // for Result
#include <arrow/status.h>       // for Status, ARROW_RETURN_NOT_OK
#include <arrow/table.h>        // for Table
#include <bits/exception.h>     // for exception
#include <iostream>             // for operator<<, basic_ostream, ostream
#include <memory>               // for shared_ptr, __shared_ptr_access
#include <string>               // for operator<<, string
#include <vector>               // for vector

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

/**
 * @brief If any specific files (index/statistics/summary) are passed for
 * inspection, inspect them using the functions in inspect_files
 */
arrow::Status run_arguments(cli_args args);

int main(int argc, char *argv[])
{
    const cli_args &args = read_options(argc, argv);

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

    std::cout << "Starting to read SSTables: \n";
    //auto result = sstable_to_arrow::read_sstables(args.sstable_dir_path);
    auto result = sstable_to_arrow::scan_sstable(args.sstable_dir_path);
    //ARROW_ASSIGN_OR_RAISE(auto sstables, sstable_to_arrow::scan_sstable(args.sstable_dir_path));
    EXIT_NOT_OK(result.status(), "error reading sstables");
    auto sstables = result.ValueOrDie();
    //std::shared_ptr<arrow::RecordBatchReader> sstables = std::move(result).ValueOrDie();


    if (args.write_parquet){
        std::cout << "Starting to write parquet: \n";
        EXIT_NOT_OK(sstable_to_arrow::io::write_parquet(args.parquet_dst_path, sstables), "error writing parquet");
    }
    //TODO: support streaming mode for send
    //if (args.listen)
    //    EXIT_NOT_OK(sstable_to_arrow::io::send_tables(sstables), "error sending tables");
    //else
    //    for (auto &entry : sstables)
    //        std::cout << entry->ToString() << '\n';

    return 0;
}

arrow::Status run_arguments(cli_args args)
{
    if (args.statistics_only)
    {
        sstable_to_arrow::file_container<sstable_statistics_t> statistics;
        statistics.set_path(args.statistics_path);
        ARROW_RETURN_NOT_OK(statistics.init());
        sstable_to_arrow::debug_statistics(statistics.file());
    }
    else if (args.index_only)
    {
        sstable_to_arrow::file_container<sstable_index_t> index;
        index.set_path(args.index_path);
        ARROW_RETURN_NOT_OK(index.init());
        sstable_to_arrow::debug_index(index.file());
    }
    else if (args.summary_only)
    {
        sstable_to_arrow::file_container<sstable_summary_t> summary;
        summary.set_path(args.summary_path);
        ARROW_RETURN_NOT_OK(summary.init());
        sstable_to_arrow::debug_summary(summary.file());
    }
    return arrow::Status::OK();
}
