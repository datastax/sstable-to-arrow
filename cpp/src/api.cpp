#include "api.h"
#include "opts.h"                               // for get_file_paths_from_...
#include "sstable.h"                            // for sstable_t
#include "sstable_to_arrow.h"                   // for vector_to_columnar_t...
#include <arrow/filesystem/s3fs.h>              // for EnsureS3Initialized
#include <arrow/status.h>                       // for Status, ARROW_RETURN...
#include <boost/algorithm/string/predicate.hpp> // for istarts_with
#include <ext/alloc_traits.h>                   // for __alloc_traits<>::va...
#include <iostream>                             // for operator<<, basic_os...
#include <utility>                              // for pair
namespace arrow { class Table; }

namespace sstable_to_arrow
{
namespace
{
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
} // namespace

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

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> read_sstables(std::string_view path)
{
    if (boost::istarts_with(path, "s3://"))
    {
        s3_connection conn;
        ARROW_ASSIGN_OR_RAISE(auto sstables, get_file_paths_from_s3(path));
        return convert_sstables(sstables);
    }
    else
    {
        auto sstables{get_file_paths_from_local(path)};
        return convert_sstables(sstables);
    }
}

} // namespace sstable_to_arrow
