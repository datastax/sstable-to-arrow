// See http://arrow.apache.org/docs/cpp/examples/row_columnar_conversion.html

#include "sstable_to_arrow.h"

column_t::column_t(
    const std::string &name_,
    const std::string &cassandra_type_,
    std::shared_ptr<arrow::DataType> type_,
    arrow::MemoryPool *pool,
    bool complex_ts_allowed)
    : cassandra_type(cassandra_type_), field(arrow::field(name_, type_))
{
    auto status = arrow::MakeBuilder(pool, field->type(), &builder);
    if (!status.ok())
    {
        std::cerr << "error making builder for column " << field->name() << '\n';
        exit(1);
    }

    // TODO currently calling get_arrow_type twice, potentially expensive
    // see if there is easier way to replace
    auto ts_type = complex_ts_allowed ? conversions::get_arrow_type(cassandra_type, true) : arrow::timestamp(arrow::TimeUnit::MICRO);
    status = arrow::MakeBuilder(pool, ts_type, &ts_builder);
    if (!status.ok())
    {
        std::cerr << "error making timestamp builder for column " << field->name() << '\n';
        exit(1);
    }
}

arrow::Status column_t::reserve(uint32_t nrows)
{
    ARROW_RETURN_NOT_OK(reserve_builder(builder.get(), nrows));
    ARROW_RETURN_NOT_OK(reserve_builder(ts_builder.get(), nrows));
    return arrow::Status::OK();
}

conversion_helper_t::conversion_helper_t(std::shared_ptr<sstable_statistics_t> sstable_statistics, arrow::MemoryPool *pool)
{
    auto &statistics_ptr = (*sstable_statistics->toc()->array())[2];
    statistics = dynamic_cast<sstable_statistics_t::statistics_t *>(statistics_ptr->body());
    assert(statistics != nullptr);

    metadata = get_serialization_header(sstable_statistics);
    assert(metadata != nullptr);

    // partition key
    partition_key = std::make_shared<column_t>(
        "partition key",
        metadata->partition_key_type()->body(),
        pool,
        false); // false -> we don't create nested fields for the timestamps,
                // since we use this column to store the row timestamp

    // clustering columns
    for (auto &col : *metadata->clustering_key_types()->array())
        clustering_cols.push_back(std::make_shared<column_t>(
            "clustering key",
            col->body(),
            pool));

    // static and regular columns
    for (auto &col : *metadata->static_columns()->array())
        static_cols.push_back(std::make_shared<column_t>(
            col->name()->body(),
            col->column_type()->body(),
            pool));

    for (auto &col : *metadata->regular_columns()->array())
        regular_cols.push_back(std::make_shared<column_t>(
            col->name()->body(),
            col->column_type()->body(),
            pool));
}

uint64_t conversion_helper_t::get_timestamp(uint64_t delta) const
{
    return metadata->min_timestamp()->val() + conversions::TIMESTAMP_EPOCH + delta;
}

size_t conversion_helper_t::num_data_cols() const
{
    // partition key
    return 1 + clustering_cols.size() + static_cols.size() + regular_cols.size();
}

size_t conversion_helper_t::num_ts_cols() const
{
    return 1 + static_cols.size() + regular_cols.size();
}

arrow::Status conversion_helper_t::reserve()
{
    size_t nrows = statistics->number_of_rows() + 5; // for security
    ARROW_RETURN_NOT_OK(partition_key->reserve(nrows));
    for (auto group : {clustering_cols, static_cols, regular_cols})
        for (auto col : group)
            ARROW_RETURN_NOT_OK(col->reserve(nrows));
    return arrow::Status::OK();
}

std::shared_ptr<arrow::Schema> conversion_helper_t::schema() const
{
    arrow::FieldVector schema_vec(num_data_cols() + num_ts_cols());
    size_t i = 0;

    schema_vec[i++] = partition_key->field;
    schema_vec[i++] = arrow::field("_ts__row", partition_key->ts_builder->type());

    for (auto &col : clustering_cols)
        schema_vec[i++] = col->field;

    for (auto &group : {static_cols, regular_cols})
    {
        for (auto &col : group)
        {
            schema_vec[i++] = col->field;
            schema_vec[i++] = arrow::field("_ts__" + col->field->name(), col->ts_builder->type());
        }
    }

    return arrow::schema(schema_vec);
}

arrow::Result<std::shared_ptr<arrow::Table>> conversion_helper_t::to_table() const
{
    arrow::ArrayVector finished_arrays(num_data_cols() + num_ts_cols());
    size_t i = 0;

    partition_key->builder->Finish(&finished_arrays[i++]);
    partition_key->ts_builder->Finish(&finished_arrays[i++]);

    for (auto &col : clustering_cols)
        ARROW_RETURN_NOT_OK(col->builder->Finish(&finished_arrays[i++]));

    for (auto &group : {static_cols, regular_cols})
    {
        for (auto &col : group)
        {
            ARROW_RETURN_NOT_OK(col->builder->Finish(&finished_arrays[i++]));
            ARROW_RETURN_NOT_OK(col->ts_builder->Finish(&finished_arrays[i++]));
        }
    }

    assert(i == num_data_cols() + num_ts_cols());
    return arrow::Table::Make(schema(), finished_arrays);
}

arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Table> *table, arrow::MemoryPool *pool)
{
    auto start_ts = std::chrono::high_resolution_clock::now();
    auto start = std::chrono::time_point_cast<std::chrono::microseconds>(start_ts).time_since_epoch().count();

    auto helper = std::make_unique<conversion_helper_t>(statistics, pool);
    helper->reserve();

    std::string_view partition_key_type = helper->partition_key->cassandra_type;
    auto partition_key_builder = helper->partition_key->builder.get();

    for (auto &partition : *sstable->partitions())
    {
        // std::cout << "deletion time: " <<  std::hex << (deletion_time->local_deletion_time()) << '\n';
        // auto deletion_ts = partition->header()->deletion_time()->marked_for_delete_at();
        // auto now = std::chrono::high_resolution_clock::now().time_since_epoch();
        // uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(now).count();

        // if (us >= deletion_ts)
        // {
        //     std::cout << "found deleted partition, skipping\n";
        //     continue;
        // }

        auto partition_key = partition->header()->key();

        for (auto &unfiltered : *partition->unfiltereds())
        {
            if ((unfiltered->flags() & 0x01) != 0) // end of partition
                break;
            else if ((unfiltered->flags() & 0x02) != 0) // range tombstone
                process_marker(dynamic_cast<sstable_data_t::range_tombstone_marker_t *>(unfiltered->body()));
            else // row
            {
                ARROW_RETURN_NOT_OK(append_scalar(partition_key_type, partition_key_builder, partition_key, pool));
                auto row = dynamic_cast<sstable_data_t::row_t *>(unfiltered->body());
                bool is_static = ((unfiltered->flags() & 0x80) != 0) && ((row->extended_flags() & 0x01) != 0);
                process_row(row, is_static, helper, pool);
            }
        }
    }

    // finish the arrays and store them into a vector
    ARROW_ASSIGN_OR_RAISE(*table, helper->to_table());

    std::cout << "\n===== table =====\n"
              << (*table)->ToString() << "==========\n";

    auto end_ts = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::time_point_cast<std::chrono::microseconds>(end_ts).time_since_epoch().count();
    std::cout << "[PROFILE conversion]: " << (end - start) << "us\n";

    return arrow::Status::OK();
}

arrow::Status process_marker(sstable_data_t::range_tombstone_marker_t *marker)
{
    std::cout << "MARKER FOUND\n";
    return arrow::Status::OK();
}

arrow::Status reserve_builder(arrow::ArrayBuilder *builder, const int64_t &nrows)
{
    DEBUG_ONLY(std::cout << "reserving for " << builder->type()->ToString() << '\n');
    ARROW_RETURN_NOT_OK(builder->Reserve(nrows));
    for (int i = 0; i < builder->num_children(); ++i)
        ARROW_RETURN_NOT_OK(reserve_builder(builder->child(i), nrows));
    return arrow::Status::OK();
}

arrow::Status process_row(
    sstable_data_t::row_t *row,
    bool is_static,
    const std::unique_ptr<conversion_helper_t> &helper,
    arrow::MemoryPool *pool)
{
    // get the row timestamp info
    auto ts_builder = dynamic_cast<arrow::TimestampBuilder *>(helper->partition_key->ts_builder.get());
    if (row->_is_null_liveness_info())
        ts_builder->AppendNull();
    else
    {
        uint64_t delta = row->liveness_info()->delta_timestamp()->val();
        ts_builder->Append(helper->get_timestamp(delta));
    }

    // ignore timestamps for clustering cols since they don't have them
    if (is_static)
    {
        for (auto col : helper->clustering_cols)
            col->builder->AppendNull();
    }
    else
    {
        // all non-static rows should have non-null clustering blocks, even if
        // they are empty
        assert(!row->_is_null_clustering_blocks());
        // all clustering cols should be present in a row (only tombstones have
        // null values in the clustering cols)
        assert(row->clustering_blocks()->values()->size() == helper->clustering_cols.size());
        for (int i = 0; i < row->clustering_blocks()->values()->size(); ++i)
        {
            auto &cell = (*row->clustering_blocks()->values())[i];
            auto &col = helper->clustering_cols[i];
            ARROW_RETURN_NOT_OK(append_scalar(col->cassandra_type, col->builder.get(), cell, pool));
        }
    }

    for (int i = 0, cell_idx = 0; i < helper->static_cols.size(); ++i)
    {
        if (is_static && does_cell_exist(row, i))
            ARROW_RETURN_NOT_OK(append_cell((*row->cells())[cell_idx++].get(), helper, helper->static_cols[i], pool));
        else
        {
            ARROW_RETURN_NOT_OK(helper->static_cols[i]->builder->AppendNull());
            ARROW_RETURN_NOT_OK(helper->static_cols[i]->ts_builder->AppendNull());
        }
    }

    // parse each of the row's cells
    for (int i = 0, cell_idx = 0; i < helper->regular_cols.size(); ++i)
    {
        if (!is_static && does_cell_exist(row, i)) // this cell is not missing
            ARROW_RETURN_NOT_OK(append_cell((*row->cells())[cell_idx++].get(), helper, helper->regular_cols[i], pool));
        else
        {
            ARROW_RETURN_NOT_OK(helper->regular_cols[i]->builder->AppendNull());
            ARROW_RETURN_NOT_OK(helper->regular_cols[i]->ts_builder->AppendNull());
        }
    }

    return arrow::Status::OK();
}

arrow::Status append_cell(kaitai::kstruct *cell, const std::unique_ptr<conversion_helper_t> &helper, std::shared_ptr<column_t> col, arrow::MemoryPool *pool)
{
    if (conversions::is_multi_cell(col->cassandra_type))
        return append_complex(
            col,
            helper,
            dynamic_cast<sstable_data_t::complex_cell_t *>(cell),
            pool);
    else
        return append_simple(
            col,
            helper,
            dynamic_cast<sstable_data_t::simple_cell_t *>(cell),
            pool);
}

arrow::Status append_complex(std::shared_ptr<column_t> col, const std::unique_ptr<conversion_helper_t> &helper, const sstable_data_t::complex_cell_t *cell, arrow::MemoryPool *pool)
{
    if (conversions::is_map(col->cassandra_type))
    {
        // cast builders and create new map inside builder
        auto builder = dynamic_cast<arrow::MapBuilder *>(col->builder.get());
        ARROW_RETURN_NOT_OK(builder->Append());
        auto ts_builder = dynamic_cast<arrow::MapBuilder *>(col->ts_builder.get());
        ARROW_RETURN_NOT_OK(ts_builder->Append());
        auto ts_item_builder = dynamic_cast<arrow::TimestampBuilder *>(ts_builder->item_builder());

        std::string_view key_type, value_type;
        for (const auto &simple_cell : *cell->simple_cells())
        {
            // keys are stored in the cell path
            conversions::get_map_child_types(col->cassandra_type, &key_type, &value_type);
            ARROW_RETURN_NOT_OK(append_scalar(key_type, builder->key_builder(), simple_cell->path()->value(), pool));
            ARROW_RETURN_NOT_OK(append_scalar(value_type, builder->item_builder(), simple_cell->value(), pool));

            ARROW_RETURN_NOT_OK(append_scalar(key_type, ts_builder->key_builder(), simple_cell->path()->value(), pool));
            ARROW_RETURN_NOT_OK(append_ts(ts_item_builder, helper, simple_cell.get()));
        }
    }
    else if (conversions::is_set(col->cassandra_type))
    {
        // cast builders and create new sublist inside builder
        auto builder = dynamic_cast<arrow::ListBuilder *>(col->builder.get());
        ARROW_RETURN_NOT_OK(builder->Append());
        auto ts_builder = dynamic_cast<arrow::ListBuilder *>(col->ts_builder.get());
        ARROW_RETURN_NOT_OK(ts_builder->Append());
        auto ts_value_builder = dynamic_cast<arrow::TimestampBuilder *>(ts_builder->value_builder());

        for (const auto &simple_cell : *cell->simple_cells())
        {
            // values of a set are stored in the path, while the actual cell value is empty
            ARROW_RETURN_NOT_OK(append_scalar(conversions::get_child_type(col->cassandra_type), builder->value_builder(), simple_cell->path()->value(), pool));
            ARROW_RETURN_NOT_OK(append_ts(ts_value_builder, helper, simple_cell.get()));
        }
    }
    else if (conversions::is_list(col->cassandra_type))
    {
        // cast builders and create new sublist inside builder
        auto builder = dynamic_cast<arrow::ListBuilder *>(col->builder.get());
        ARROW_RETURN_NOT_OK(builder->Append());
        auto ts_builder = dynamic_cast<arrow::ListBuilder *>(col->ts_builder.get());
        ARROW_RETURN_NOT_OK(ts_builder->Append());
        auto ts_value_builder = dynamic_cast<arrow::TimestampBuilder *>(ts_builder->value_builder());

        for (const auto &simple_cell : *cell->simple_cells())
        {
            ARROW_RETURN_NOT_OK(append_scalar(conversions::get_child_type(col->cassandra_type), builder->value_builder(), simple_cell->value(), pool));
            ARROW_RETURN_NOT_OK(append_ts(ts_value_builder, helper, simple_cell.get()));
        }
    }
    else
    {
        return arrow::Status::TypeError("Unknown complex type ", col->cassandra_type);
    }

    return arrow::Status::OK();
}

arrow::Status append_simple(std::shared_ptr<column_t> col, const std::unique_ptr<conversion_helper_t> &helper, sstable_data_t::simple_cell_t *cell, arrow::MemoryPool *pool)
{
    auto timestamp_builder = dynamic_cast<arrow::TimestampBuilder *>(col->ts_builder.get());
    ARROW_RETURN_NOT_OK(append_ts(timestamp_builder, helper, cell));
    return append_scalar(col->cassandra_type, col->builder.get(), cell->value(), pool);
}

/**
 * @brief Appends a scalar value to an Arrow ArrayBuilder corresponding to a certain CQL type given by `coltype`.
 * 
 * @param coltype the CQL data type of the column
 * @param builder_ptr a pointer to the arrow ArrayBuilder
 * @param bytes a buffer containing the bytes from the SSTable
 */
arrow::Status append_scalar(std::string_view coltype, arrow::ArrayBuilder *builder_ptr, std::string_view bytes, arrow::MemoryPool *pool)
{
    DEBUG_ONLY(std::cout << "appending to vector: " << coltype << " (builder capacity " << builder_ptr->capacity() << ")\n");

    // for all other types, we parse the data using kaitai, which might end up
    // being a performance bottleneck
    // TODO look into potential uses of memcpy for optimization
    std::string buffer(bytes);
    kaitai::kstream ks(buffer);

    if (conversions::is_composite(coltype))
    {
        auto builder = dynamic_cast<arrow::StructBuilder *>(builder_ptr);
        ARROW_RETURN_NOT_OK(builder->Append());

        auto maybe_tree = conversions::parse_nested_type(coltype);
        auto tree = *maybe_tree;
        for (int i = 0; i < tree->children->size(); ++i)
        {
            uint16_t child_size = ks.read_u2be();
            auto data = ks.read_bytes(child_size);
            ks.read_bytes(1); // skip the '0' bit at end
            ARROW_RETURN_NOT_OK(append_scalar((*tree->children)[i]->str, builder->child(i), data, pool));
        }
        return arrow::Status::OK();
    }
    else if (conversions::is_reversed(coltype))
        return append_scalar(conversions::get_child_type(coltype), builder_ptr, bytes, pool);
    else if (coltype == "org.apache.cassandra.db.marshal.DecimalType") // decimal
    {
        auto builder = (arrow::StructBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append());
        auto scale_builder = dynamic_cast<arrow::Int32Builder *>(builder->child(0));
        auto val_builder = dynamic_cast<arrow::BinaryBuilder *>(builder->child(1));
        int scale = ks.read_s4be();
        ARROW_RETURN_NOT_OK(scale_builder->Append(scale));
        ARROW_RETURN_NOT_OK(val_builder->Append(ks.read_bytes_full()));
        return arrow::Status::OK();
    }
    else if (coltype == "org.apache.cassandra.db.marshal.DurationType") // duration
    {
        auto builder = dynamic_cast<arrow::FixedSizeListBuilder *>(builder_ptr);
        auto value_builder = dynamic_cast<arrow::Int64Builder *>(builder->value_builder());
        long long months = vint_t(&ks).val();
        long long days = vint_t(&ks).val();
        long long nanoseconds = vint_t(&ks).val();
        ARROW_RETURN_NOT_OK(builder->Append());
        ARROW_RETURN_NOT_OK(value_builder->Append(months));
        ARROW_RETURN_NOT_OK(value_builder->Append(days));
        ARROW_RETURN_NOT_OK(value_builder->Append(nanoseconds));
        return arrow::Status::OK();
    }
    else if (coltype == "org.apache.cassandra.db.marshal.InetAddressType") // inet
    {
        auto builder = dynamic_cast<arrow::DenseUnionBuilder *>(builder_ptr);
        if (ks.size() == 4)
        {
            builder->Append(0);
            auto ipv4_builder = dynamic_cast<arrow::Int32Builder *>(builder->child(0));
            return ipv4_builder->Append(ks.read_s4be());
        }
        else if (ks.size() == 8)
        {
            builder->Append(1);
            auto ipv6_builder = dynamic_cast<arrow::Int64Builder *>(builder->child(1));
            return ipv6_builder->Append(ks.read_s8be());
        }
        else
        {
            std::cerr << "invalid IP address of size " << ks.size() << " bytes. needs to be 4 or 8\n";
            return arrow::Status::TypeError("invalid IP address");
        }
    }
    else if (coltype == "org.apache.cassandra.db.marshal.IntegerType") // varint
    {
        auto builder = dynamic_cast<arrow::Int64Builder *>(builder_ptr);
        return builder->Append(vint_t::parse_java(bytes.data(), bytes.size()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.SimpleDateType") // date
    {
        auto builder = dynamic_cast<arrow::Date32Builder *>(builder_ptr);
        uint32_t date = ks.read_u4be() - (1 << 31);
        return builder->Append(date);
    }

    // handle "primitive" types with a macro
#define APPEND_TO_BUILDER(cassandra_type, arrow_type, read_size)                   \
    else if (coltype == "org.apache.cassandra.db.marshal." #cassandra_type "Type") \
    {                                                                              \
        auto builder = dynamic_cast<arrow::arrow_type##Builder *>(builder_ptr);    \
        return builder->Append(ks.read_##read_size());                             \
    }

    APPEND_TO_BUILDER(Ascii, String, bytes_full)
    APPEND_TO_BUILDER(Boolean, Boolean, u1)
    APPEND_TO_BUILDER(Byte, Int8, s1)
    APPEND_TO_BUILDER(Bytes, Binary, bytes_full)
    APPEND_TO_BUILDER(Double, Double, f8be)
    APPEND_TO_BUILDER(Float, Float, f4be)
    APPEND_TO_BUILDER(Int32, Int32, s4be)
    APPEND_TO_BUILDER(LexicalUUID, FixedSizeBinary, bytes_full)
    APPEND_TO_BUILDER(Long, Int64, s8be)
    APPEND_TO_BUILDER(Short, Int16, s2be)
    APPEND_TO_BUILDER(Time, Time64, s8be)
    APPEND_TO_BUILDER(Timestamp, Timestamp, s8be)
    APPEND_TO_BUILDER(TimeUUID, FixedSizeBinary, bytes_full)
    APPEND_TO_BUILDER(UTF8, String, bytes_full)
    APPEND_TO_BUILDER(UUID, FixedSizeBinary, bytes_full)

#undef APPEND_TO_BUILDER

    return arrow::Status::TypeError(std::string("unrecognized type when appending to arrow array builder: ") + std::string(coltype));
}

arrow::Status append_ts(arrow::TimestampBuilder *builder, const std::unique_ptr<conversion_helper_t> &helper, sstable_data_t::simple_cell_t *cell)
{
    if (cell->_is_null_delta_timestamp())
        return builder->AppendNull();
    else
    {
        uint64_t delta = cell->delta_timestamp()->val();
        return builder->Append(helper->get_timestamp(delta));
    }
}

// Read the serialization header from the statistics file.
sstable_statistics_t::serialization_header_t *get_serialization_header(std::shared_ptr<sstable_statistics_t> statistics)
{
    const auto &toc = *statistics->toc()->array();
    const auto &ptr = toc[3]; // 3 is the index of the serialization header in the table of contents in the statistics file
    return dynamic_cast<sstable_statistics_t::serialization_header_t *>(ptr->body());
}

// idx is the index of the desired column in the superset of columns contained in this SSTable
bool does_cell_exist(sstable_data_t::row_t *row, const uint64_t &idx)
{
    return row->_is_null_columns_bitmask() || (row->columns_bitmask()->bitmask & (1 << idx)) == 0;
}
