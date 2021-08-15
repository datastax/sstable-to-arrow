#include "conversion_helper.h"
#include "conversions.h"              // for get_arrow_type, get_arrow_type...
#include "opts.h"                     // for flags, global_flags, DEBUG_ONLY
#include "vint.h"                     // for vint_t
#include <algorithm>                  // for max
#include <arrow/array/builder_dict.h> // for NumericBuilder
#include <arrow/table.h>              // for Table
#include <arrow/type.h>               // for Field, DataType, Schema (ptr o...
#include <assert.h>                   // for assert
#include <cstdint>
#include <ext/alloc_traits.h>    // for __alloc_traits<>::value_type
#include <initializer_list>      // for initializer_list
#include <kaitai/kaitaistruct.h> // for kstruct
namespace arrow
{
class Array;
class MemoryPool;
} // namespace arrow

namespace sstable_to_arrow
{

arrow::Status column_t::init(arrow::MemoryPool *pool, bool complex_ts_allowed)
{
    // create data builder
    ARROW_RETURN_NOT_OK(arrow::MakeBuilder(pool, field->type(), &builder));
    if (has_second)
        ARROW_RETURN_NOT_OK(arrow::MakeBuilder(pool, field->type(), &second));

    if (has_metadata())
    {
        // TODO currently calling get_arrow_type multiple times, potentially
        // expensive see if there is easier way to replace since we already know
        // base data type
        auto micro = arrow::timestamp(arrow::TimeUnit::MICRO);
        conversions::get_arrow_type_options options;
        options.replace_with = micro;
        options.for_cudf = global_flags.for_cudf;
        ARROW_ASSIGN_OR_RAISE(const auto &ts_type,
                              complex_ts_allowed ? conversions::get_arrow_type(cassandra_type, options) : micro);
        ARROW_RETURN_NOT_OK(arrow::MakeBuilder(pool, ts_type, &ts_builder));
        ARROW_RETURN_NOT_OK(arrow::MakeBuilder(pool, ts_type, &local_del_time_builder));

        auto ttl = arrow::duration(arrow::TimeUnit::SECOND);
        options.replace_with = ttl;
        options.for_cudf = global_flags.for_cudf;
        ARROW_ASSIGN_OR_RAISE(const auto &ttl_type,
                              complex_ts_allowed ? conversions::get_arrow_type(cassandra_type, options) : ttl);
        ARROW_RETURN_NOT_OK(arrow::MakeBuilder(pool, ttl_type, &ttl_builder));
    }

    return arrow::Status::OK();
}

// Reserve enough space in each of the builders for `nrows` elements.
arrow::Status column_t::reserve(uint32_t nrows)
{
    ARROW_RETURN_NOT_OK(reserve_builder(builder.get(), nrows));
    if (has_second)
        ARROW_RETURN_NOT_OK(reserve_builder(second.get(), nrows));

    if (has_metadata())
        for (auto builder_ptr : {ts_builder.get(), local_del_time_builder.get(), ttl_builder.get()})
            ARROW_RETURN_NOT_OK(reserve_builder(builder_ptr, nrows));
    return arrow::Status::OK();
}

// DANGEROUS - ensure that the next four elements in `ptr` are allocated!
arrow::Result<uint8_t> column_t::finish(std::shared_ptr<arrow::Array> *ptr)
{
    uint8_t n_cols_finished = 0;

#define NEXT_ITEM (ptr + n_cols_finished++)
    ARROW_RETURN_NOT_OK(builder->Finish(NEXT_ITEM));
    if (has_second)
        ARROW_RETURN_NOT_OK(second->Finish(NEXT_ITEM));

    if (has_metadata())
    {
        ARROW_RETURN_NOT_OK(ts_builder->Finish(NEXT_ITEM));
        ARROW_RETURN_NOT_OK(local_del_time_builder->Finish(NEXT_ITEM));
        ARROW_RETURN_NOT_OK(ttl_builder->Finish(NEXT_ITEM));
    }
#undef NEXT_ITEM

    return n_cols_finished;
}

uint8_t column_t::append_to_schema(std::shared_ptr<arrow::Field> *schema) const
{
    return append_to_schema(schema, field->name());
}

uint8_t column_t::append_to_schema(std::shared_ptr<arrow::Field> *schema, const std::string &ts_name) const
{
    uint8_t n_cols_finished = 0;

#define NEXT_ITEM *(schema + n_cols_finished++)
    if (has_second)
    {
        NEXT_ITEM = field->WithName(field->name() + "_part1");
        NEXT_ITEM = field->WithName(field->name() + "_part2"); // duplicate the type
    }
    else
        NEXT_ITEM = field;

    if (has_metadata())
    {
        NEXT_ITEM = arrow::field("_ts_" + ts_name, ts_builder->type());
        NEXT_ITEM = arrow::field("_del_time_" + ts_name, local_del_time_builder->type());
        NEXT_ITEM = arrow::field("_ttl_" + ts_name, ttl_builder->type());
    }
#undef NEXT_ITEM

    return n_cols_finished;
}

arrow::Status column_t::append_null()
{
    ARROW_RETURN_NOT_OK(builder->AppendNull());
    if (has_second)
        ARROW_RETURN_NOT_OK(second->AppendNull());

    if (global_flags.include_metadata)
        for (auto &builder_ptr : {ts_builder.get(), local_del_time_builder.get(), ttl_builder.get()})
            ARROW_RETURN_NOT_OK(builder_ptr->AppendNull());

    return arrow::Status::OK();
}

bool column_t::has_metadata() const
{
    return global_flags.include_metadata && !m_is_clustering;
}

arrow::Result<std::shared_ptr<column_t>> conversion_helper_t::make_column(const std::string &name,
                                                                          const std::string &cass_type,
                                                                          bool is_clustering)
{
    bool needs_second = false;
    conversions::get_arrow_type_options options;
    options.for_cudf = global_flags.for_cudf;
    ARROW_ASSIGN_OR_RAISE(const auto &arrow_type, conversions::get_arrow_type(cass_type, options, &needs_second));
    if (needs_second)
        ++m_n_uuid_cols;
    return std::make_shared<column_t>(name, cass_type, arrow_type, is_clustering, needs_second);
}

// initialize the name and type of the partition key column and all of the
// clustering, static, and regular columns, but do not create the builders
conversion_helper_t::conversion_helper_t(const std::unique_ptr<sstable_statistics_t> &sstable_statistics)
{
    auto &statistics_ptr = (*sstable_statistics->toc()->array())[2]; // see sstable_statistics.ksy for info
    statistics = dynamic_cast<sstable_statistics_t::statistics_t *>(statistics_ptr->body());
    assert(statistics != nullptr);

    metadata = get_serialization_header(sstable_statistics);
    assert(metadata != nullptr);
}

arrow::Result<std::unique_ptr<conversion_helper_t>> conversion_helper_t::create(
    const std::unique_ptr<sstable_statistics_t> &statistics)
{
    auto helper = std::unique_ptr<conversion_helper_t>(new conversion_helper_t(statistics));
    ARROW_RETURN_NOT_OK(helper->create_columns());
    return helper;
}

arrow::Status conversion_helper_t::create_columns()
{
    // partition key
    ARROW_ASSIGN_OR_RAISE(partition_key, make_column("partition_key", metadata->partition_key_type()->body(), false));

    // clustering columns
    const std::string clustering_key_name = "clustering_key_";
    int i = 0; // count clustering columns
    for (auto &col : *metadata->clustering_key_types()->array())
    {
        ARROW_ASSIGN_OR_RAISE(const auto &clustering_col,
                              make_column(clustering_key_name + std::to_string(i++), col->body(), true))
        clustering_cols.push_back(clustering_col);
    }

    // static and regular columns
    for (auto &col : *metadata->static_columns()->array())
    {
        ARROW_ASSIGN_OR_RAISE(const auto &static_col,
                              make_column(col->name()->body(), col->column_type()->body(), false));
        static_cols.push_back(static_col);
    }
    for (auto &col : *metadata->regular_columns()->array())
    {
        ARROW_ASSIGN_OR_RAISE(const auto &regular_col,
                              make_column(col->name()->body(), col->column_type()->body(), false));
        regular_cols.push_back(regular_col);
    }
    return arrow::Status::OK();
}

// creates the builders for each of the columns in this table
arrow::Status conversion_helper_t::init(arrow::MemoryPool *pool)
{
    // false -> we don't create nested fields for the timestamps,
    // since we use this column to store the row liveness info
    ARROW_RETURN_NOT_OK(partition_key->init(pool, false));
    if (global_flags.include_metadata)
    {
        partition_key_local_del_time =
            std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::SECOND), pool);
        partition_key_marked_for_deletion_at =
            std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::MICRO), pool);
        row_local_del_time = std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::SECOND), pool);
        row_marked_for_deletion_at =
            std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::MICRO), pool);
    }

    for (auto &col : clustering_cols)
        ARROW_RETURN_NOT_OK(col->init(pool));

    // static and regular columns
    for (auto group : {static_cols, regular_cols})
        for (auto &col : group)
            ARROW_RETURN_NOT_OK(col->init(pool));

    return arrow::Status::OK();
}

// reserves space in each of the column builders for the number of rows in this
// table
arrow::Status conversion_helper_t::reserve()
{
    size_t nrows = statistics->number_of_rows() + 5; // for security
    ARROW_RETURN_NOT_OK(partition_key->reserve(nrows));
    if (global_flags.include_metadata)
    {
        ARROW_RETURN_NOT_OK(partition_key_local_del_time->Reserve(nrows));
        ARROW_RETURN_NOT_OK(partition_key_marked_for_deletion_at->Reserve(nrows));
        ARROW_RETURN_NOT_OK(row_local_del_time->Reserve(nrows));
        ARROW_RETURN_NOT_OK(row_marked_for_deletion_at->Reserve(nrows));
    }
    for (auto group : {clustering_cols, static_cols, regular_cols})
        for (auto col : group)
            ARROW_RETURN_NOT_OK(col->reserve(nrows));
    return arrow::Status::OK();
}

// timestamps are stored in microseconds
uint64_t conversion_helper_t::get_timestamp(uint64_t delta) const
{
    return metadata->min_timestamp()->val() + conversions::TIMESTAMP_EPOCH + delta;
}
// deletion time is stored in seconds
uint64_t conversion_helper_t::get_local_del_time(uint64_t delta) const
{
    return metadata->min_local_deletion_time()->val() + conversions::DELETION_TIME_EPOCH + delta;
}
// TTL is stored in seconds
uint64_t conversion_helper_t::get_ttl(uint64_t delta) const
{
    return metadata->min_ttl()->val() + delta;
}

arrow::Status conversion_helper_t::append_partition_deletion_time(uint32_t local_deletion_time,
                                                                  uint64_t marked_for_delete_at)
{
    if (local_deletion_time == conversions::LOCAL_DELETION_TIME_NULL)
        ARROW_RETURN_NOT_OK(partition_key_local_del_time->AppendNull());
    else
        ARROW_RETURN_NOT_OK(partition_key_local_del_time->Append(local_deletion_time));

    if (marked_for_delete_at == conversions::MARKED_FOR_DELETE_AT_NULL)
        ARROW_RETURN_NOT_OK(partition_key_marked_for_deletion_at->AppendNull());
    else
        ARROW_RETURN_NOT_OK(partition_key_marked_for_deletion_at->Append(marked_for_delete_at));

    return arrow::Status::OK();
}

size_t conversion_helper_t::num_data_cols() const
{
    // 1 - partition key
    // add the number of extra columns required to store uuids
    return 1 + clustering_cols.size() + static_cols.size() + regular_cols.size() +
           (global_flags.for_cudf ? m_n_uuid_cols : 0);
}

size_t conversion_helper_t::num_ts_cols() const
{
    // +3 is the row timestamp/local_del_time/ttl
    // +4 is 2 columns each for the partition and row deletion time
    // ignore clustering columns with no timestamp
    return 3 + 4 + static_cols.size() * 3 + regular_cols.size() * 3;
}

size_t conversion_helper_t::num_cols() const
{
    size_t total = num_data_cols();
    if (global_flags.include_metadata)
        total += num_ts_cols();
    return total;
}

std::shared_ptr<arrow::Schema> conversion_helper_t::schema() const
{
    arrow::FieldVector schema_vec(num_cols());

    size_t i = partition_key->append_to_schema(schema_vec.data(), "row_liveness");

    if (global_flags.include_metadata)
    {
        schema_vec[i++] = arrow::field("_local_del_time_partition", partition_key_local_del_time->type());
        schema_vec[i++] = arrow::field("_marked_for_del_at_partition", partition_key_marked_for_deletion_at->type());
        schema_vec[i++] = arrow::field("_local_del_time_row", row_local_del_time->type());
        schema_vec[i++] = arrow::field("_marked_for_delete_at_row", row_marked_for_deletion_at->type());
    }

    // clustering columns don't have timestamps
    for (auto &col : clustering_cols)
        i += col->append_to_schema(&schema_vec[i]);

    for (auto &group : {static_cols, regular_cols})
        for (auto &col : group)
            i += col->append_to_schema(&schema_vec[i]);

    return arrow::schema(schema_vec);
}

arrow::Result<std::shared_ptr<arrow::Table>> conversion_helper_t::to_table() const
{
    arrow::ArrayVector finished_arrays(num_cols());

    ARROW_ASSIGN_OR_RAISE(size_t i, partition_key->finish(&finished_arrays[0]));

    if (global_flags.include_metadata)
    {
        ARROW_RETURN_NOT_OK(partition_key_local_del_time->Finish(&finished_arrays[i++]));
        ARROW_RETURN_NOT_OK(partition_key_marked_for_deletion_at->Finish(&finished_arrays[i++]));
        ARROW_RETURN_NOT_OK(row_local_del_time->Finish(&finished_arrays[i++]));
        ARROW_RETURN_NOT_OK(row_marked_for_deletion_at->Finish(&finished_arrays[i++]));
    }

    for (auto &col : clustering_cols)
        ARROW_RETURN_NOT_OK(col->builder->Finish(&finished_arrays[i++]));

    for (auto &group : {static_cols, regular_cols})
    {
        for (auto &col : group)
        {
            ARROW_ASSIGN_OR_RAISE(uint8_t n, col->finish(&finished_arrays[i]));
            i += n;
        }
    }

    assert(i == num_cols());
    return arrow::Table::Make(schema(), finished_arrays);
}

arrow::Status reserve_builder(arrow::ArrayBuilder *builder, const int64_t &nrows)
{
    DEBUG_ONLY("reserving for " + builder->type()->ToString() + '\n');
    ARROW_RETURN_NOT_OK(builder->Reserve(nrows));
    for (int i = 0; i < builder->num_children(); ++i)
        ARROW_RETURN_NOT_OK(reserve_builder(builder->child(i), nrows));
    return arrow::Status::OK();
}

// Read the serialization header from the statistics file.
sstable_statistics_t::serialization_header_t *get_serialization_header(
    const std::unique_ptr<sstable_statistics_t> &statistics)
{
    const auto &toc = *statistics->toc()->array();
    const auto &ptr = toc[3]; // 3 is the index of the serialization header in
                              // the table of contents in the statistics file
    return dynamic_cast<sstable_statistics_t::serialization_header_t *>(ptr->body());
}

} // namespace sstable_to_arrow
