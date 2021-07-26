// See http://arrow.apache.org/docs/cpp/examples/row_columnar_conversion.html

#include "sstable_to_arrow.h"

arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Table> *table, arrow::MemoryPool *pool)
{
    auto start_ts = std::chrono::high_resolution_clock::now();
    auto start = std::chrono::time_point_cast<std::chrono::microseconds>(start_ts).time_since_epoch().count();

    auto helper = std::make_unique<conversion_helper_t>(statistics);
    ARROW_RETURN_NOT_OK(helper->init(pool));

    std::string_view partition_key_type = helper->partition_key->cassandra_type;
    auto &partition_key_col = helper->partition_key;

    for (auto &partition : *sstable->partitions())
    {
        auto partition_key = partition->header()->key();
        auto local_deletion_time = partition->header()->deletion_time()->local_deletion_time();
        auto marked_for_delete_at = partition->header()->deletion_time()->marked_for_delete_at();

        bool no_rows = true;

        // for a partition that is not deleted,
        // loop through the rows and tombstones
        for (auto &unfiltered : *partition->unfiltereds())
        {
            if ((unfiltered->flags() & 0x01) != 0) // end of partition
                break;
            else if ((unfiltered->flags() & 0x02) != 0) // range tombstone
                process_marker(dynamic_cast<sstable_data_t::range_tombstone_marker_t *>(unfiltered->body()));
            else // row
            {
                no_rows = false;
                // append partition key
                ARROW_RETURN_NOT_OK(append_scalar(partition_key_type, partition_key_col->builder.get(), partition_key, pool));
                // append partition deletion info
                if (local_deletion_time == conversions::LOCAL_DELETION_TIME_NULL)
                    ARROW_RETURN_NOT_OK(helper->partition_key_local_del_time->AppendNull());
                else
                    ARROW_RETURN_NOT_OK(helper->partition_key_local_del_time->Append(local_deletion_time));

                if (marked_for_delete_at == conversions::MARKED_FOR_DELETE_AT_NULL)
                    ARROW_RETURN_NOT_OK(helper->partition_key_marked_for_deletion_at->AppendNull());
                else
                    ARROW_RETURN_NOT_OK(helper->partition_key_marked_for_deletion_at->Append(marked_for_delete_at));
                auto row = dynamic_cast<sstable_data_t::row_t *>(unfiltered->body());
                bool is_static = ((unfiltered->flags() & 0x80) != 0) && ((row->extended_flags() & 0x01) != 0);
                process_row(row, is_static, helper, pool);
            }
        }

        // if there are no rows (only one "end of partition" unfiltered),
        // we create a filler row containing the partition deletion information
        if (no_rows)
        {
            ARROW_RETURN_NOT_OK(append_scalar(partition_key_type, partition_key_col->builder.get(), partition_key, pool));

            if (local_deletion_time == conversions::LOCAL_DELETION_TIME_NULL)
                ARROW_RETURN_NOT_OK(helper->partition_key_local_del_time->AppendNull());
            else
                ARROW_RETURN_NOT_OK(helper->partition_key_local_del_time->Append(local_deletion_time));

            if (marked_for_delete_at == conversions::MARKED_FOR_DELETE_AT_NULL)
                ARROW_RETURN_NOT_OK(helper->partition_key_marked_for_deletion_at->AppendNull());
            else
                ARROW_RETURN_NOT_OK(helper->partition_key_marked_for_deletion_at->Append(marked_for_delete_at));

            ARROW_RETURN_NOT_OK(partition_key_col->ts_builder->AppendNull());
            ARROW_RETURN_NOT_OK(partition_key_col->local_del_time_builder->AppendNull());
            ARROW_RETURN_NOT_OK(partition_key_col->ttl_builder->AppendNull());
            ARROW_RETURN_NOT_OK(helper->row_local_del_time->AppendNull());
            ARROW_RETURN_NOT_OK(helper->row_marked_for_deletion_at->AppendNull());

            // fill all of the other columns with nulls
            for (auto &col : helper->clustering_cols)
                ARROW_RETURN_NOT_OK(col->append_null());
            for (auto &group : {helper->static_cols, helper->regular_cols})
                for (auto &col : group)
                    ARROW_RETURN_NOT_OK(col->append_null());

            continue;
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

// Add the cells and time data in `row` to the columns in `helper`.
arrow::Status process_row(
    sstable_data_t::row_t *row,
    bool is_static,
    const std::unique_ptr<conversion_helper_t> &helper,
    arrow::MemoryPool *pool)
{
    // append row deletion time info
    if (row->_is_null_deletion_time())
    {
        ARROW_RETURN_NOT_OK(helper->row_local_del_time->AppendNull());
        ARROW_RETURN_NOT_OK(helper->row_marked_for_deletion_at->AppendNull());
    }
    else
    {
        ARROW_RETURN_NOT_OK(helper->row_local_del_time->Append(helper->get_local_del_time(row->deletion_time()->delta_local_deletion_time()->val())));
        ARROW_RETURN_NOT_OK(helper->row_marked_for_deletion_at->Append(helper->get_timestamp(row->deletion_time()->delta_marked_for_delete_at()->val())));
    }

    // get the row timestamp info, which is stored in the builders of the partition key column
    auto ts_builder = dynamic_cast<column_t::ts_builder_t *>(helper->partition_key->ts_builder.get());
    auto local_del_time_builder = dynamic_cast<column_t::local_del_time_builder_t *>(helper->partition_key->local_del_time_builder.get());
    auto ttl_builder = dynamic_cast<column_t::ttl_builder_t *>(helper->partition_key->ttl_builder.get());
    if (row->_is_null_liveness_info())
    {
        ARROW_RETURN_NOT_OK(ts_builder->AppendNull());
        ARROW_RETURN_NOT_OK(local_del_time_builder->AppendNull());
        ARROW_RETURN_NOT_OK(ttl_builder->AppendNull());
    }
    else
    {
        // the liveness info exists, so we add each field to the column if it exists
        auto liveness_info = row->liveness_info();
        uint64_t delta = liveness_info->delta_timestamp()->val();
        ARROW_RETURN_NOT_OK(ts_builder->Append(helper->get_timestamp(delta)));

        if (liveness_info->_is_null_primary_key_liveness_deletion_time())
            ARROW_RETURN_NOT_OK(local_del_time_builder->AppendNull());
        else
        {
            delta = liveness_info->primary_key_liveness_deletion_time()->val();
            ARROW_RETURN_NOT_OK(local_del_time_builder->Append(helper->get_local_del_time(delta)));
        }

        if (liveness_info->_is_null_delta_ttl())
            ARROW_RETURN_NOT_OK(ttl_builder->AppendNull());
        else
        {
            delta = liveness_info->delta_ttl()->val();
            ARROW_RETURN_NOT_OK(ttl_builder->Append(helper->get_ttl(delta)));
        }
    }

    // handle clustering columns
    // static rows don't have clustering columns
    if (is_static)
    {
        for (auto col : helper->clustering_cols)
            ARROW_RETURN_NOT_OK(col->append_null());
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
            // ignore timestamps for clustering cols since they don't have them
        }
    }

    // handle static columns
    // i is the index in the SSTable's static columns,
    // while cell_idx is the index in the row's vector of cells, since some
    // columns might be missing
    for (int i = 0, cell_idx = 0; i < helper->static_cols.size(); ++i)
    {
        if (is_static && does_cell_exist(row, i))
            ARROW_RETURN_NOT_OK(append_cell((*row->cells())[cell_idx++].get(), helper, helper->static_cols[i], pool));
        else
            ARROW_RETURN_NOT_OK(helper->static_cols[i]->append_null());
    }

    // handle regular columns
    // see above re `i` and `cell_idx`
    for (int i = 0, cell_idx = 0; i < helper->regular_cols.size(); ++i)
    {
        if (!is_static && does_cell_exist(row, i))
            ARROW_RETURN_NOT_OK(append_cell((*row->cells())[cell_idx++].get(), helper, helper->regular_cols[i], pool));
        else
            ARROW_RETURN_NOT_OK(helper->regular_cols[i]->append_null());
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

        // map each cell path to an individual timestamp/local_del_time/ttl,
        // assuming that they are maps
        // TODO please abstract this
        auto ts_builder = dynamic_cast<arrow::MapBuilder *>(col->ts_builder.get());
        auto local_del_time_builder = dynamic_cast<arrow::MapBuilder *>(col->local_del_time_builder.get());
        auto ttl_builder = dynamic_cast<arrow::MapBuilder *>(col->ttl_builder.get());
        ARROW_RETURN_NOT_OK(ts_builder->Append());
        ARROW_RETURN_NOT_OK(local_del_time_builder->Append());
        ARROW_RETURN_NOT_OK(ttl_builder->Append());
        auto ts_item_builder = dynamic_cast<column_t::ts_builder_t *>(ts_builder->item_builder());
        auto local_del_time_item_builder = dynamic_cast<column_t::local_del_time_builder_t *>(local_del_time_builder->item_builder());
        auto ttl_item_builder = dynamic_cast<column_t::ttl_builder_t *>(ttl_builder->item_builder());

        std::string_view key_type, value_type;
        for (const auto &simple_cell : *cell->simple_cells())
        {
            // map keys are stored in the cell path
            conversions::get_map_child_types(col->cassandra_type, &key_type, &value_type);
            ARROW_RETURN_NOT_OK(append_scalar(key_type, builder->key_builder(), simple_cell->path()->value(), pool));
            ARROW_RETURN_NOT_OK(append_scalar(value_type, builder->item_builder(), simple_cell->value(), pool));

            ARROW_RETURN_NOT_OK(append_scalar(key_type, ts_builder->key_builder(), simple_cell->path()->value(), pool));
            ARROW_RETURN_NOT_OK(append_ts_if_exists(ts_item_builder, helper, simple_cell.get()));

            ARROW_RETURN_NOT_OK(append_scalar(key_type, local_del_time_builder->key_builder(), simple_cell->path()->value(), pool));
            ARROW_RETURN_NOT_OK(append_local_del_time_if_exists(local_del_time_item_builder, helper, simple_cell.get()));

            ARROW_RETURN_NOT_OK(append_scalar(key_type, ttl_builder->key_builder(), simple_cell->path()->value(), pool));
            ARROW_RETURN_NOT_OK(append_ttl_if_exists(ttl_item_builder, helper, simple_cell.get()));
        }
    }
    else if (conversions::is_set(col->cassandra_type))
    {
        // sets are handled essentially identically to lists,
        // except getting the value from the cell path instead of the cell value
        // cast builders and create new sublist inside builder
        auto builder = dynamic_cast<arrow::ListBuilder *>(col->builder.get());
        ARROW_RETURN_NOT_OK(builder->Append());

        auto ts_builder = dynamic_cast<arrow::ListBuilder *>(col->ts_builder.get());
        auto local_del_time_builder = dynamic_cast<arrow::ListBuilder *>(col->local_del_time_builder.get());
        auto ttl_builder = dynamic_cast<arrow::ListBuilder *>(col->ttl_builder.get());
        ARROW_RETURN_NOT_OK(ts_builder->Append());
        ARROW_RETURN_NOT_OK(local_del_time_builder->Append());
        ARROW_RETURN_NOT_OK(ttl_builder->Append());
        auto ts_value_builder = dynamic_cast<column_t::ts_builder_t *>(ts_builder->value_builder());
        auto local_del_time_value_builder = dynamic_cast<column_t::local_del_time_builder_t *>(local_del_time_builder->value_builder());
        auto ttl_value_builder = dynamic_cast<column_t::ttl_builder_t *>(ttl_builder->value_builder());

        for (const auto &simple_cell : *cell->simple_cells())
        {
            // values of a set are stored in the path, while the actual cell value is empty
            ARROW_RETURN_NOT_OK(append_scalar(conversions::get_child_type(col->cassandra_type), builder->value_builder(), simple_cell->path()->value(), pool));
            ARROW_RETURN_NOT_OK(append_ts_if_exists(ts_value_builder, helper, simple_cell.get()));
            ARROW_RETURN_NOT_OK(append_local_del_time_if_exists(local_del_time_value_builder, helper, simple_cell.get()));
            ARROW_RETURN_NOT_OK(append_ttl_if_exists(ttl_value_builder, helper, simple_cell.get()));
        }
    }
    else if (conversions::is_list(col->cassandra_type))
    {
        // cast builders and create new sublist inside builder
        auto builder = dynamic_cast<arrow::ListBuilder *>(col->builder.get());
        ARROW_RETURN_NOT_OK(builder->Append());

        auto ts_builder = dynamic_cast<arrow::ListBuilder *>(col->ts_builder.get());
        auto local_del_time_builder = dynamic_cast<arrow::ListBuilder *>(col->local_del_time_builder.get());
        auto ttl_builder = dynamic_cast<arrow::ListBuilder *>(col->ttl_builder.get());
        ARROW_RETURN_NOT_OK(ts_builder->Append());
        ARROW_RETURN_NOT_OK(local_del_time_builder->Append());
        ARROW_RETURN_NOT_OK(ttl_builder->Append());
        auto ts_value_builder = dynamic_cast<column_t::ts_builder_t *>(ts_builder->value_builder());
        auto local_del_time_value_builder = dynamic_cast<column_t::local_del_time_builder_t *>(local_del_time_builder->value_builder());
        auto ttl_value_builder = dynamic_cast<column_t::ttl_builder_t *>(ttl_builder->value_builder());

        for (const auto &simple_cell : *cell->simple_cells())
        {
            ARROW_RETURN_NOT_OK(append_scalar(conversions::get_child_type(col->cassandra_type), builder->value_builder(), simple_cell->value(), pool));
            ARROW_RETURN_NOT_OK(append_ts_if_exists(ts_value_builder, helper, simple_cell.get()));
            ARROW_RETURN_NOT_OK(append_local_del_time_if_exists(local_del_time_value_builder, helper, simple_cell.get()));
            ARROW_RETURN_NOT_OK(append_ttl_if_exists(ttl_value_builder, helper, simple_cell.get()));
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
    auto ts_builder = dynamic_cast<column_t::ts_builder_t *>(col->ts_builder.get());
    auto local_del_time_builder = dynamic_cast<column_t::local_del_time_builder_t *>(col->local_del_time_builder.get());
    auto ttl_builder = dynamic_cast<column_t::ttl_builder_t *>(col->ttl_builder.get());
    ARROW_RETURN_NOT_OK(append_ts_if_exists(ts_builder, helper, cell));
    ARROW_RETURN_NOT_OK(append_local_del_time_if_exists(local_del_time_builder, helper, cell));
    ARROW_RETURN_NOT_OK(append_ttl_if_exists(ttl_builder, helper, cell));
    return append_scalar(col->cassandra_type, col->builder.get(), cell->value(), pool);
}

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

arrow::Status append_ts_if_exists(column_t::ts_builder_t *builder, const std::unique_ptr<conversion_helper_t> &helper, sstable_data_t::simple_cell_t *cell)
{
    if (cell->_is_null_delta_timestamp())
        return builder->AppendNull();
    else
    {
        uint64_t delta = cell->delta_timestamp()->val();
        return builder->Append(helper->get_timestamp(delta));
    }
}
arrow::Status append_local_del_time_if_exists(column_t::local_del_time_builder_t *builder, const std::unique_ptr<conversion_helper_t> &helper, sstable_data_t::simple_cell_t *cell)
{
    if (cell->_is_null_delta_local_deletion_time())
        return builder->AppendNull();
    else
    {
        uint64_t delta = cell->delta_local_deletion_time()->val();
        return builder->Append(helper->get_local_del_time(delta));
    }
}
arrow::Status append_ttl_if_exists(column_t::ttl_builder_t *builder, const std::unique_ptr<conversion_helper_t> &helper, sstable_data_t::simple_cell_t *cell)
{
    if (cell->_is_null_delta_ttl())
        return builder->AppendNull();
    else
    {
        uint64_t delta = cell->delta_ttl()->val();
        return builder->Append(helper->get_ttl(delta));
    }
}

// idx is the index of the desired column in the superset of columns contained in this SSTable
bool does_cell_exist(sstable_data_t::row_t *row, const uint64_t &idx)
{
    return row->_is_null_columns_bitmask() || (row->columns_bitmask()->bitmask & (1 << idx)) == 0;
}
