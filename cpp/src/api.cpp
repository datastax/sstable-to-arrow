#include "api.h"
#include "opts.h"                               // for get_file_paths_from_...
#include "sstable.h"                            // for sstable_t
#include "sstable_to_arrow.h"                   // for vector_to_columnar_t...
#include <arrow/filesystem/s3fs.h>              // for EnsureS3Initialized
#include <arrow/status.h>                       // for Status, ARROW_RETURN...
#include "arrow/record_batch.h"                 // for RecordBatchReader
#include "arrow/table.h"                        // for Table, CombineChunksToBatch
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


struct SSTableRecordBatchReader : public arrow::RecordBatchReader
{
    SSTableRecordBatchReader(std::map<int, std::shared_ptr<sstable_t>> sstables,
                             std::shared_ptr<arrow::Schema> output_schema, arrow::MemoryPool *pool)
        : sstable_i(0), offset(0), output_schema(output_schema), pool(pool), sstable_ids(sstables.size()),
          sstables(sstables.size())
    {
        int i = 0;
        for (auto &entry : sstables)
        {
            this->sstable_ids[i] = entry.first;
            this->sstables[i] = entry.second;
            i++;
        }
    }

    std::shared_ptr<arrow::Schema> schema() const
    {
        return output_schema;
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch)
    {
        // Get current sstable
        int sstable_id = sstable_ids[sstable_i];

        if (sstables.size() <= sstable_i){
            *batch = nullptr;
            return arrow::Status::OK();
        }

        std::shared_ptr<sstable_t> sstable = sstables[sstable_i];
        std::cout << "========== Reading Chunk from SSTable #" << sstable_id << " ==========\n";

        std::cout << "Fetching data\n";
        if (offset == 0){
        ARROW_RETURN_NOT_OK(sstable->fetch_data());
        }
        std::cout << "Converting to columnar table\n";
        // Load as a table
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> in_table,
                              vector_to_columnar_table(sstable->statistics(), sstable->data()));

        // Promote the schema
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table,
                              arrow::PromoteTableToSchema(in_table, output_schema, pool));

        // Make into a batch (should be zero-copy)
        ARROW_ASSIGN_OR_RAISE(*batch, table->CombineChunksToBatch(pool));

        // TODO: bump if we're done with the current sstable
        if (sstable->data()->is_eof()){
          sstable_i++;
        }

        return arrow::Status::OK();
    }

    int32_t sstable_i;
    int32_t offset;
    std::shared_ptr<arrow::Schema> output_schema;
    arrow::MemoryPool *pool;
    std::vector<int> sstable_ids;
    std::vector<std::shared_ptr<sstable_t>> sstables;
};

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> scan_sstable(std::string_view path)
{
    std::map<int, std::shared_ptr<sstable_t>> sstables;
    if (boost::istarts_with(path, "s3://"))
    {
        s3_connection conn;
        ARROW_ASSIGN_OR_RAISE(sstables, get_file_paths_from_s3(path));
    }
    else
    {
        sstables = get_file_paths_from_local(path);
    }

    // Get unified schema
    std::vector<std::shared_ptr<sstable_t>>all_tables(sstables.size());
    int i = 0;
    for (auto &entry : sstables)
    {
        ARROW_RETURN_NOT_OK(entry.second->init_stats());
        all_tables[i] = entry.second;
        i++;
    }
    std::shared_ptr<arrow::Schema> output_schema;
    ARROW_ASSIGN_OR_RAISE(output_schema, common_arrow_schema(all_tables));

    return std::make_shared<SSTableRecordBatchReader>(sstables, output_schema, arrow::default_memory_pool());
}

} // namespace sstable_to_arrow
