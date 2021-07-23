#include "conversion_helper.h"

/**
 * @brief create the data builder and time data builders for this column
 * 
 * @param complex_ts_allowed Whether the type of the time data builders should
 * mirror the data builder if complex. For example, if this column stores lists
 * of data and complex_ts_allowed is set to true, each of the time data builders
 * will also store a list of data. Otherwise, they will store a single timestamp
 * for the entire list.
 * @return arrow::Status 
 */
arrow::Status column_t::init(arrow::MemoryPool *pool, bool complex_ts_allowed)
{
    // create data builder
    ARROW_RETURN_NOT_OK(arrow::MakeBuilder(pool, field->type(), &builder));

    // TODO currently calling get_arrow_type multiple times, potentially expensive
    // see if there is easier way to replace since we already know base data type
    auto micro = arrow::timestamp(arrow::TimeUnit::MICRO);
    auto ts_type = complex_ts_allowed ? conversions::get_arrow_type(cassandra_type, conversions::get_arrow_type_options{micro}) : micro;
    ARROW_RETURN_NOT_OK(arrow::MakeBuilder(pool, ts_type, &ts_builder));
    ARROW_RETURN_NOT_OK(arrow::MakeBuilder(pool, ts_type, &local_del_time_builder));

    auto ttl = arrow::duration(arrow::TimeUnit::SECOND);
    auto ttl_type = complex_ts_allowed ? conversions::get_arrow_type(cassandra_type, conversions::get_arrow_type_options{ttl}) : ttl;
    ARROW_RETURN_NOT_OK(arrow::MakeBuilder(pool, ttl_type, &ttl_builder));

    return arrow::Status::OK();
}

// Reserve enough space in each of the builders for `nrows` elements.
arrow::Status column_t::reserve(uint32_t nrows)
{
    for (auto builder_ptr : {builder.get(), ts_builder.get(), local_del_time_builder.get(), ttl_builder.get()})
        ARROW_RETURN_NOT_OK(reserve_builder(builder_ptr, nrows));
    return arrow::Status::OK();
}

// DANGEROUS - ensure that the next four elements in `ptr` are allocated!
arrow::Status column_t::finish(std::shared_ptr<arrow::Array> *ptr)
{
    ARROW_RETURN_NOT_OK(builder->Finish(ptr));
    ARROW_RETURN_NOT_OK(ts_builder->Finish(ptr + 1));
    ARROW_RETURN_NOT_OK(local_del_time_builder->Finish(ptr + 2));
    ARROW_RETURN_NOT_OK(ttl_builder->Finish(ptr + 3));
    return arrow::Status::OK();
}

arrow::Status column_t::append_null()
{
    for (auto &builder_ptr : {builder.get(), ts_builder.get(), local_del_time_builder.get(), ttl_builder.get()})
        ARROW_RETURN_NOT_OK(builder_ptr->AppendNull());
    return arrow::Status::OK();
}

// initialize the name and type of the partition key column and all of the
// clustering, static, and regular columns, but do not create the builders
conversion_helper_t::conversion_helper_t(std::shared_ptr<sstable_statistics_t> sstable_statistics)
{
    auto &statistics_ptr = (*sstable_statistics->toc()->array())[2]; // see sstable_statistics.ksy for info
    statistics = dynamic_cast<sstable_statistics_t::statistics_t *>(statistics_ptr->body());
    assert(statistics != nullptr);

    metadata = get_serialization_header(sstable_statistics);
    assert(metadata != nullptr);

    // partition key
    partition_key = std::make_shared<column_t>(
        "partition key",
        metadata->partition_key_type()->body());

    // clustering columns
    std::string clustering_key_name = "clustering key ";
    int i = 0;
    for (auto &col : *metadata->clustering_key_types()->array())
        clustering_cols.push_back(std::make_shared<column_t>(
            clustering_key_name + std::to_string(i++), // TODO expensive string concatentation
            col->body()));

    // static and regular columns
    for (auto &col : *metadata->static_columns()->array())
        static_cols.push_back(std::make_shared<column_t>(
            col->name()->body(),
            col->column_type()->body()));

    for (auto &col : *metadata->regular_columns()->array())
        regular_cols.push_back(std::make_shared<column_t>(
            col->name()->body(),
            col->column_type()->body()));
}

// creates the builders for each of the columns in this table
arrow::Status conversion_helper_t::init(arrow::MemoryPool *pool)
{
    // false -> we don't create nested fields for the timestamps,
    // since we use this column to store the row timestamp
    ARROW_RETURN_NOT_OK(partition_key->init(pool, false));

    for (auto &col : clustering_cols)
        ARROW_RETURN_NOT_OK(col->init(pool));

    // static and regular columns
    for (auto group : {static_cols, regular_cols})
        for (auto &col : group)
            ARROW_RETURN_NOT_OK(col->init(pool));

    return arrow::Status::OK();
}

// reserves space in each of the column builders for the number of rows in this table
arrow::Status conversion_helper_t::reserve()
{
    size_t nrows = statistics->number_of_rows() + 5; // for security
    ARROW_RETURN_NOT_OK(partition_key->reserve(nrows));
    for (auto group : {clustering_cols, static_cols, regular_cols})
        for (auto col : group)
            ARROW_RETURN_NOT_OK(col->reserve(nrows));
    return arrow::Status::OK();
}

uint64_t conversion_helper_t::get_timestamp(uint64_t delta) const
{
    return metadata->min_timestamp()->val() + conversions::TIMESTAMP_EPOCH + delta;
}

uint64_t conversion_helper_t::get_local_del_time(uint64_t delta) const
{
    return metadata->min_local_deletion_time()->val() + conversions::DELETION_TIME_EPOCH + delta;
}

uint64_t conversion_helper_t::get_ttl(uint64_t delta) const
{
    return metadata->min_ttl()->val() + delta;
}

size_t conversion_helper_t::num_data_cols() const
{
    // partition key
    return 1 + clustering_cols.size() + static_cols.size() + regular_cols.size();
}

size_t conversion_helper_t::num_ts_cols() const
{
    // +3 is the row timestamp/local_del_time/ttl
    return 3 + static_cols.size() * 3 + regular_cols.size() * 3;
}

std::shared_ptr<arrow::Schema> conversion_helper_t::schema() const
{
    arrow::FieldVector schema_vec(num_data_cols() + num_ts_cols());
    size_t i = 0;

    schema_vec[i++] = partition_key->field;
    schema_vec[i++] = arrow::field("_ts_row", partition_key->ts_builder->type());
    schema_vec[i++] = arrow::field("_del_time_row", partition_key->local_del_time_builder->type());
    schema_vec[i++] = arrow::field("_ttl_row", partition_key->ttl_builder->type());

    // clustering columns don't have timestamps
    for (auto &col : clustering_cols)
        schema_vec[i++] = col->field;

    for (auto &group : {static_cols, regular_cols})
    {
        for (auto &col : group)
        {
            schema_vec[i++] = col->field;
            schema_vec[i++] = arrow::field("_ts_" + col->field->name(), col->ts_builder->type());
            schema_vec[i++] = arrow::field("_del_time_" + col->field->name(), col->local_del_time_builder->type());
            schema_vec[i++] = arrow::field("_ttl_" + col->field->name(), col->ttl_builder->type());
        }
    }

    return arrow::schema(schema_vec);
}

arrow::Result<std::shared_ptr<arrow::Table>> conversion_helper_t::to_table() const
{
    arrow::ArrayVector finished_arrays(num_data_cols() + num_ts_cols());

    partition_key->finish(&finished_arrays[0]);

    size_t i = 4;
    for (auto &col : clustering_cols)
        ARROW_RETURN_NOT_OK(col->builder->Finish(&finished_arrays[i++]));

    for (auto &group : {static_cols, regular_cols})
    {
        for (auto &col : group)
        {
            ARROW_RETURN_NOT_OK(col->finish(&finished_arrays[i]));
            i += 4;
        }
    }

    assert(i == num_data_cols() + num_ts_cols());
    return arrow::Table::Make(schema(), finished_arrays);
}

arrow::Status reserve_builder(arrow::ArrayBuilder *builder, const int64_t &nrows)
{
    DEBUG_ONLY(std::cout << "reserving for " << builder->type()->ToString() << '\n');
    ARROW_RETURN_NOT_OK(builder->Reserve(nrows));
    for (int i = 0; i < builder->num_children(); ++i)
        ARROW_RETURN_NOT_OK(reserve_builder(builder->child(i), nrows));
    return arrow::Status::OK();
}

// Read the serialization header from the statistics file.
sstable_statistics_t::serialization_header_t *get_serialization_header(std::shared_ptr<sstable_statistics_t> statistics)
{
    const auto &toc = *statistics->toc()->array();
    const auto &ptr = toc[3]; // 3 is the index of the serialization header in the table of contents in the statistics file
    return dynamic_cast<sstable_statistics_t::serialization_header_t *>(ptr->body());
}
