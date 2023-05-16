#include "sstable_to_arrow.h"
#include "clustering_blocks.h"          // for clustering_blocks_t
#include "columns_bitmask.h"            // for columns_bitmask_t
#include "conversions.h"                // for get_child_type, get_map_chil...
#include "deletion_time.h"              // for deletion_time_t
#include "opts.h"                       // for flags, global_flags
#include "vint.h"                       // for vint_t
#include <algorithm>                    // for copy
#include <arrow/array/builder_binary.h> // for StringBuilder, BinaryBuilder
#include <arrow/array/builder_dict.h>   // for ArrayBuilder, NumericBuilder
#include <arrow/array/builder_nested.h> // for ListBuilder, MapBuilder, Str...
#include <arrow/array/builder_union.h>  // for DenseUnionBuilder
#include <assert.h>                     // for assert
#include <boost/algorithm/hex.hpp>      // for hex
#include <ext/alloc_traits.h>           // for __alloc_traits<>::value_type
#include <initializer_list>             // for initializer_list
#include <iostream>                     // for operator<<, cout, ostream
#include <kaitai/kaitaistream.h>        // for kstream
#include <kaitai/kaitaistruct.h>        // for kstruct
#include <stddef.h>                     // for size_t
#include <stdexcept>                    // for out_of_range
#include <string>                       // for string, char_traits, operator+
#include <unordered_map>                // for unordered_map
#include <vector>                       // for vector
#include "sstable.h"
#include "streaming_sstable_data.h"

class sstable_statistics_t;             // lines 31-31
namespace arrow
{
class MemoryPool;
class Table;
} // namespace arrow

namespace sstable_to_arrow
{

arrow::Result<std::shared_ptr<arrow::Table>> streaming_vector_to_columnar_table(
    const std::unique_ptr<sstable_statistics_t> &statistics, 
    kaitai::kstream* m__io,
    arrow::MemoryPool *pool)
{
    ARROW_ASSIGN_OR_RAISE(auto helper, conversion_helper_t::create(statistics));
    ARROW_RETURN_NOT_OK(helper->init(pool));

    auto root = std::make_unique<streaming_sstable_data_t>(m__io);
    //m__io->seek(157738602);

    auto m_deserialization_helper = std::unique_ptr<deserialization_helper_t>(new deserialization_helper_t(m__io));
    auto m_partitions = std::unique_ptr<std::vector<std::unique_ptr<streaming_sstable_data_t::partition_t>>>(new std::vector<std::unique_ptr<streaming_sstable_data_t::partition_t>>());
    time_t now = time(nullptr);
    {
        std::cout << "Time: " << ctime(&now) << " - Getting a step worth of partitions\n";
        int i = 0;
        int stepsize = 1000;
        //int stepsize = 1;
        while (!m__io->is_eof() && i < stepsize) {
            if (m__io->is_eof())
            {
                std::cout << "Done\n";
            }
            try{
            auto partition = std::move(std::unique_ptr<streaming_sstable_data_t::partition_t>(
                new streaming_sstable_data_t::partition_t(m__io, nullptr, root.get())
            ));
            // This part could be parallelized
            ARROW_RETURN_NOT_OK(process_partition(partition,helper,pool));
            }catch(...){
                std::cout << "failed to process partition, this is likely a bug \n";
            }
            i++;
        }
    } 

    // finish the arrays and store them into a vector
    return helper->to_table();
}

arrow::Result<std::shared_ptr<arrow::Table>> vector_to_columnar_table(
    const std::unique_ptr<sstable_statistics_t> &statistics, 
    const std::unique_ptr<streaming_sstable_data_t> &sstable,
    arrow::MemoryPool *pool)
{
    ARROW_ASSIGN_OR_RAISE(auto helper, conversion_helper_t::create(statistics));
    ARROW_RETURN_NOT_OK(helper->init(pool));

    for (const auto &partition : *sstable->partitions())
        ARROW_RETURN_NOT_OK(process_partition(partition, helper, pool));

    // finish the arrays and store them into a vector
    return helper->to_table();
}

arrow::Result<std::shared_ptr<arrow::Schema>> common_arrow_schema(
    const std::vector<std::shared_ptr<sstable_t>>& tables
) {

    std::vector<std::shared_ptr<arrow::Schema>> schemas(tables.size());

    arrow::MemoryPool *pool = arrow::default_memory_pool();

    for (int i = 0; i < schemas.size(); ++i) {
        const auto& statistics = tables[i]->statistics();
        ARROW_ASSIGN_OR_RAISE(auto helper, conversion_helper_t::create(statistics));
        // TODO: Maybe need to init too?
        helper->init(pool);
        auto schema = helper->schema();
        schemas[i] = schema;
    }

    return arrow::UnifySchemas(schemas);
}

arrow::Status process_partition(const std::unique_ptr<streaming_sstable_data_t::partition_t> &partition,
                                std::unique_ptr<conversion_helper_t> &helper, arrow::MemoryPool *pool)
{
    const std::string &partition_key = partition->header()->key();
    uint32_t local_deletion_time = partition->header()->deletion_time()->local_deletion_time();
    uint64_t marked_for_delete_at = partition->header()->deletion_time()->marked_for_delete_at();

    bool no_rows = true;

    // for a partition that is not deleted,
    // loop through the rows and tombstones
    int i = 0;
    for (auto &unfiltered : *partition->unfiltereds())
    {
        if ((unfiltered->flags() & 0x01) != 0) // end of partition
            break;
        else if ((unfiltered->flags() & 0x02) != 0){ // range tombstone
            auto marker = dynamic_cast<streaming_sstable_data_t::range_tombstone_marker_t *>(unfiltered->body());
            if (marker == nullptr){
                break;
            }
            process_marker(marker);
        }
        else // row
        {
            no_rows = false;
            ARROW_RETURN_NOT_OK(append_scalar(helper->partition_key, partition_key, pool));

            //if (partition_key == "0x5e100d63a93eca86ea44bbe274b469ac4f1b63e4c4317fcbe16d2bcd0b990adb")
            //  std::cout << "partition key " << partition_key << "\n" ;
            //if (i == 245){
              //std::cout << i <<"th row" << "\n" ;
            //}
            //std::cout << "partition key " << partition_key << "\n" ;
            //std::cout << i <<"th row" << "\n" ;
            // append partition deletion info
            if (global_flags.include_metadata)
                ARROW_RETURN_NOT_OK(helper->append_partition_deletion_time(local_deletion_time, marked_for_delete_at));
            auto row = dynamic_cast<streaming_sstable_data_t::row_t *>(unfiltered->body());
            bool is_static = ((unfiltered->flags() & 0x80) != 0) && ((row->extended_flags() & 0x01) != 0);
            ARROW_RETURN_NOT_OK(process_row(row, is_static, helper, pool));
            i++;
        }
    }

    // if there are no rows (only one "end of partition" unfiltered),
    // we create a filler row containing the partition deletion information
    if (no_rows)
    {
        ARROW_RETURN_NOT_OK(append_scalar(helper->partition_key, partition_key, pool));

        if (global_flags.include_metadata)
        {
            ARROW_RETURN_NOT_OK(helper->append_partition_deletion_time(local_deletion_time, marked_for_delete_at));
            ARROW_RETURN_NOT_OK(helper->row_local_del_time->AppendNull());
            ARROW_RETURN_NOT_OK(helper->row_marked_for_deletion_at->AppendNull());
            ARROW_RETURN_NOT_OK(helper->partition_key->ts_builder->AppendNull());
            ARROW_RETURN_NOT_OK(helper->partition_key->local_del_time_builder->AppendNull());
            ARROW_RETURN_NOT_OK(helper->partition_key->ttl_builder->AppendNull());
        }

        // fill all of the other columns with nulls
        for (auto &col : helper->clustering_cols)
            ARROW_RETURN_NOT_OK(col->append_null());
        for (auto &group : {helper->static_cols, helper->regular_cols})
            for (auto &col : group)
                ARROW_RETURN_NOT_OK(col->append_null());
    }

    return arrow::Status::OK();
}

arrow::Status process_marker(streaming_sstable_data_t::range_tombstone_marker_t *marker)
{
    (void)marker;
    std::cout << "MARKER FOUND\n";
    std::cout << marker->kind();
    std::cout << marker->start_deletion_time();
    std::cout << marker->deletion_time();
    std::cout << marker->end_deletion_time();
    std::cout << "\n";
    return arrow::Status::OK();
}

// Add the cells and time data in `row` to the columns in `helper`.
arrow::Status process_row(streaming_sstable_data_t::row_t *row, bool is_static,
                          const std::unique_ptr<conversion_helper_t> &helper, arrow::MemoryPool *pool)
{
    // append row deletion time info
    if (global_flags.include_metadata)
    {
        if (row->_is_null_deletion_time())
        {
            ARROW_RETURN_NOT_OK(helper->row_local_del_time->AppendNull());
            ARROW_RETURN_NOT_OK(helper->row_marked_for_deletion_at->AppendNull());
        }
        else
        {
            ARROW_RETURN_NOT_OK(helper->row_local_del_time->Append(
                helper->get_local_del_time(row->deletion_time()->delta_local_deletion_time()->val())));
            ARROW_RETURN_NOT_OK(helper->row_marked_for_deletion_at->Append(
                helper->get_timestamp(row->deletion_time()->delta_marked_for_delete_at()->val())));
        }

        // get the row timestamp info, which is stored in the builders of the
        // partition key column
        auto ts_builder = dynamic_cast<column_t::ts_builder_t *>(helper->partition_key->ts_builder.get());
        auto local_del_time_builder =
            dynamic_cast<column_t::local_del_time_builder_t *>(helper->partition_key->local_del_time_builder.get());
        auto ttl_builder = dynamic_cast<column_t::ttl_builder_t *>(helper->partition_key->ttl_builder.get());
        if (row->_is_null_liveness_info())
        {
            ARROW_RETURN_NOT_OK(ts_builder->AppendNull());
            ARROW_RETURN_NOT_OK(local_del_time_builder->AppendNull());
            ARROW_RETURN_NOT_OK(ttl_builder->AppendNull());
        }
        else
        {
            // the liveness info exists, so we add each field to the column if it
            // exists
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
        for (size_t i = 0; i < row->clustering_blocks()->values()->size(); ++i)
        {
            auto &cell = (*row->clustering_blocks()->values())[i];
            auto &col = helper->clustering_cols[i];
            ARROW_RETURN_NOT_OK(append_scalar(col, cell, pool));
            // ignore timestamps for clustering cols since they don't have them
        }
    }

    // handle static columns
    // i is the index in the SSTable's static columns,
    // while cell_idx is the index in the row's vector of cells, since some
    // columns might be missing
    for (size_t i = 0, cell_idx = 0; i < helper->static_cols.size(); ++i)
    {
        if (is_static && does_cell_exist(row, i))
            ARROW_RETURN_NOT_OK(append_cell((*row->cells())[cell_idx++].get(), helper, helper->static_cols[i], pool));
        else
            ARROW_RETURN_NOT_OK(helper->static_cols[i]->append_null());
    }

    // handle regular columns
    // see above re `i` and `cell_idx`
    for (size_t i = 0, cell_idx = 0; i < helper->regular_cols.size(); ++i)
    {
        if (!is_static && does_cell_exist(row, i)){
            //std::cout << "cell number " << cell_idx << "\n";
            //std::cout << "cell exists: " ;
            if(row->cells()->size() > cell_idx)
                ARROW_RETURN_NOT_OK(append_cell((*row->cells())[cell_idx++].get(), helper, helper->regular_cols[i], pool));
            else{
                //std::cout << "row size check fails" << "\n";
                ARROW_RETURN_NOT_OK(helper->regular_cols[i]->append_null());
            }
        }
        else{
            //std::cout << "cell does not exist" << "\n";
            ARROW_RETURN_NOT_OK(helper->regular_cols[i]->append_null());
        }
    }

    return arrow::Status::OK();
}

arrow::Status append_cell(kaitai::kstruct *cell, const std::unique_ptr<conversion_helper_t> &helper,
                          std::shared_ptr<column_t> col, arrow::MemoryPool *pool)
{
    if (conversions::is_multi_cell(col->cassandra_type))
        return append_complex(col, helper, dynamic_cast<streaming_sstable_data_t::complex_cell_t *>(cell), pool);
    else
        //TODO stop here for the bad cell
        return append_simple(col, helper, dynamic_cast<streaming_sstable_data_t::simple_cell_t *>(cell), pool);
}

template <typename T>
arrow::Status initialize_ts_map_builder(const std::unique_ptr<arrow::ArrayBuilder> &from_ptr,
                                        arrow::MapBuilder **builder_ptr, T **item_ptr)
{
    *builder_ptr = dynamic_cast<arrow::MapBuilder *>(from_ptr.get());
    ARROW_RETURN_NOT_OK((*builder_ptr)->Append());
    *item_ptr = dynamic_cast<T *>((*builder_ptr)->item_builder());
    return arrow::Status::OK();
}

template <typename T>
arrow::Status initialize_ts_list_builder(const std::unique_ptr<arrow::ArrayBuilder> &from_ptr,
                                         arrow::ListBuilder **builder_ptr, T **item_ptr)
{
    *builder_ptr = dynamic_cast<arrow::ListBuilder *>(from_ptr.get());
    ARROW_RETURN_NOT_OK((*builder_ptr)->Append());
    *item_ptr = dynamic_cast<T *>((*builder_ptr)->value_builder());
    return arrow::Status::OK();
}

arrow::Status append_complex(std::shared_ptr<column_t> col, const std::unique_ptr<conversion_helper_t> &helper,
                             const streaming_sstable_data_t::complex_cell_t *cell, arrow::MemoryPool *pool)
{
    if (conversions::is_map(col->cassandra_type))
    {
        // cast builders and create new map inside builder
        auto builder = dynamic_cast<arrow::MapBuilder *>(col->builder.get());
        ARROW_RETURN_NOT_OK(builder->Append());

        // map each cell path to an individual timestamp/local_del_time/ttl,
        // assuming that they are maps
        // TODO please abstract this
        arrow::MapBuilder *ts_builder{nullptr}, *local_del_time_builder{nullptr}, *ttl_builder{nullptr};
        column_t::ts_builder_t *ts_item_builder{nullptr};
        column_t::local_del_time_builder_t *local_del_time_item_builder{nullptr};
        column_t::ttl_builder_t *ttl_item_builder{nullptr};
        if (global_flags.include_metadata)
        {
            ARROW_RETURN_NOT_OK(initialize_ts_map_builder(col->ts_builder, &ts_builder, &ts_item_builder));
            ARROW_RETURN_NOT_OK(initialize_ts_map_builder(col->local_del_time_builder, &local_del_time_builder,
                                                          &local_del_time_item_builder));
            ARROW_RETURN_NOT_OK(initialize_ts_map_builder(col->ttl_builder, &ttl_builder, &ttl_item_builder));
        }

        std::string_view key_type, value_type;
        for (const auto &simple_cell : *cell->simple_cells())
        {
            // map keys are stored in the cell path
            conversions::get_map_child_types(col->cassandra_type, &key_type, &value_type);
            ARROW_RETURN_NOT_OK(
                append_scalar(key_type, builder->key_builder(), nullptr, simple_cell->path()->value(), pool));
            ARROW_RETURN_NOT_OK(
                append_scalar(value_type, builder->item_builder(), nullptr, simple_cell->value(), pool));

            if (global_flags.include_metadata)
            {
                ARROW_RETURN_NOT_OK(
                    append_scalar(key_type, ts_builder->key_builder(), nullptr, simple_cell->path()->value(), pool));
                ARROW_RETURN_NOT_OK(append_ts_if_exists(ts_item_builder, helper, simple_cell.get()));

                ARROW_RETURN_NOT_OK(append_scalar(key_type, local_del_time_builder->key_builder(), nullptr,
                                                  simple_cell->path()->value(), pool));
                ARROW_RETURN_NOT_OK(
                    append_local_del_time_if_exists(local_del_time_item_builder, helper, simple_cell.get()));

                ARROW_RETURN_NOT_OK(
                    append_scalar(key_type, ttl_builder->key_builder(), nullptr, simple_cell->path()->value(), pool));
                ARROW_RETURN_NOT_OK(append_ttl_if_exists(ttl_item_builder, helper, simple_cell.get()));
            }
        }
    }
    else if (conversions::is_set(col->cassandra_type))
    {
        // sets are handled essentially identically to lists,
        // except getting the value from the cell path instead of the cell value
        // cast builders and create new sublist inside builder
        auto builder = dynamic_cast<arrow::ListBuilder *>(col->builder.get());
        ARROW_RETURN_NOT_OK(builder->Append());

        arrow::ListBuilder *ts_builder{nullptr}, *local_del_time_builder{nullptr}, *ttl_builder{nullptr};
        column_t::ts_builder_t *ts_value_builder{nullptr};
        column_t::local_del_time_builder_t *local_del_time_value_builder{nullptr};
        column_t::ttl_builder_t *ttl_value_builder{nullptr};
        if (global_flags.include_metadata)
        {
            ARROW_RETURN_NOT_OK(initialize_ts_list_builder(col->ts_builder, &ts_builder, &ts_value_builder));
            ARROW_RETURN_NOT_OK(initialize_ts_list_builder(col->local_del_time_builder, &local_del_time_builder,
                                                           &local_del_time_value_builder));
            ARROW_RETURN_NOT_OK(initialize_ts_list_builder(col->ttl_builder, &ttl_builder, &ttl_value_builder));
        }

        for (const auto &simple_cell : *cell->simple_cells())
        {
            // values of a set are stored in the path, while the actual cell
            // value is empty
            ARROW_RETURN_NOT_OK(append_scalar(conversions::get_child_type(col->cassandra_type),
                                              builder->value_builder(), nullptr, simple_cell->path()->value(), pool));
            if (global_flags.include_metadata)
            {
                ARROW_RETURN_NOT_OK(append_ts_if_exists(ts_value_builder, helper, simple_cell.get()));
                ARROW_RETURN_NOT_OK(
                    append_local_del_time_if_exists(local_del_time_value_builder, helper, simple_cell.get()));
                ARROW_RETURN_NOT_OK(append_ttl_if_exists(ttl_value_builder, helper, simple_cell.get()));
            }
        }
    }
    else if (conversions::is_list(col->cassandra_type))
    {
        // cast builders and create new sublist inside builder
        auto builder = dynamic_cast<arrow::ListBuilder *>(col->builder.get());
        ARROW_RETURN_NOT_OK(builder->Append());

        arrow::ListBuilder *ts_builder{nullptr}, *local_del_time_builder{nullptr}, *ttl_builder{nullptr};
        column_t::ts_builder_t *ts_value_builder{nullptr};
        column_t::local_del_time_builder_t *local_del_time_value_builder{nullptr};
        column_t::ttl_builder_t *ttl_value_builder{nullptr};
        if (global_flags.include_metadata)
        {
            ARROW_RETURN_NOT_OK(initialize_ts_list_builder(col->ts_builder, &ts_builder, &ts_value_builder));
            ARROW_RETURN_NOT_OK(initialize_ts_list_builder(col->local_del_time_builder, &local_del_time_builder,
                                                           &local_del_time_value_builder));
            ARROW_RETURN_NOT_OK(initialize_ts_list_builder(col->ttl_builder, &ttl_builder, &ttl_value_builder));
        }

        std::string_view child_type = conversions::get_child_type(col->cassandra_type);
        for (const auto &simple_cell : *cell->simple_cells())
        {
            ARROW_RETURN_NOT_OK(
                append_scalar(child_type, builder->value_builder(), nullptr, simple_cell->value(), pool));
            if (global_flags.include_metadata)
            {
                ARROW_RETURN_NOT_OK(append_ts_if_exists(ts_value_builder, helper, simple_cell.get()));
                ARROW_RETURN_NOT_OK(
                    append_local_del_time_if_exists(local_del_time_value_builder, helper, simple_cell.get()));
                ARROW_RETURN_NOT_OK(append_ttl_if_exists(ttl_value_builder, helper, simple_cell.get()));
            }
        }
    }
    else
    {
        return arrow::Status::TypeError("Unknown complex type ", col->cassandra_type);
    }

    return arrow::Status::OK();
}

arrow::Status append_simple(std::shared_ptr<column_t> col, const std::unique_ptr<conversion_helper_t> &helper,
                            streaming_sstable_data_t::simple_cell_t *cell, arrow::MemoryPool *pool)
{
    //std::cout << col->field->name() << "\n";

    if (cell->_is_null_value()){
        col->append_null();
        return arrow::Status::OK();
    }

    if (global_flags.include_metadata)
    {
        auto ts_builder = dynamic_cast<column_t::ts_builder_t *>(col->ts_builder.get());
        auto local_del_time_builder =
            dynamic_cast<column_t::local_del_time_builder_t *>(col->local_del_time_builder.get());
        auto ttl_builder = dynamic_cast<column_t::ttl_builder_t *>(col->ttl_builder.get());
        ARROW_RETURN_NOT_OK(append_ts_if_exists(ts_builder, helper, cell));
        ARROW_RETURN_NOT_OK(append_local_del_time_if_exists(local_del_time_builder, helper, cell));
        ARROW_RETURN_NOT_OK(append_ttl_if_exists(ttl_builder, helper, cell));
    }
    return append_scalar(col, cell->value(), pool);
}

namespace
{
arrow::Status append_scalar(std::shared_ptr<column_t> col, std::string_view value, arrow::MemoryPool *pool)
{
    if (value == ""){
        col->append_null();
        //std::cout << "this is a tombstone\n";
        return arrow::Status::OK();                                                                                    \
    }
    return append_scalar(col->cassandra_type, col->builder.get(), col->has_second ? col->second.get() : nullptr, value,
                         pool);
}

arrow::Status append_scalar(std::string_view coltype, arrow::ArrayBuilder *builder_ptr, arrow::ArrayBuilder *second_ptr,
                            std::string_view bytes, arrow::MemoryPool *pool)
{
    // for all other types, we parse the data using kaitai, which might end up
    // being a performance bottleneck
    // TODO look into potential uses of memcpy for optimization
    /*
    if (bytes.length() > 10000){
        std::cout << "very big cell " << bytes.length()  << " bytes \n";
    }
    */

    auto ks = kaitai::kstream(std::string(bytes));

    if (conversions::is_composite(coltype))
    {
        auto builder = dynamic_cast<arrow::StructBuilder *>(builder_ptr);
        ARROW_RETURN_NOT_OK(builder->Append());
        decltype(builder) second_builder = second_ptr ? dynamic_cast<arrow::StructBuilder *>(second_ptr) : nullptr;
        if (second_ptr)
            ARROW_RETURN_NOT_OK(second_builder->Append());

        ARROW_ASSIGN_OR_RAISE(auto tree, conversions::parse_nested_type(coltype));
        for (size_t i = 0; i < tree->children->size(); ++i)
        {
            uint16_t child_size = ks.read_u2be();
            auto data = ks.read_bytes(child_size);
            ks.read_bytes(1); // skip the '0' bit at end
            ARROW_RETURN_NOT_OK(append_scalar((*tree->children)[i]->str, builder->child(i),
                                              second_ptr ? second_builder->child(i) : nullptr, data, pool));
        }
        return arrow::Status::OK();
    }
    else if (conversions::is_reversed(coltype))
        return append_scalar(conversions::get_child_type(coltype), builder_ptr, second_ptr, bytes, pool);
    else if (coltype == conversions::types::DecimalType) // decimal
    {
        int scale = ks.read_s4be();
        auto val = ks.read_bytes_full();
        auto append_decimal = [scale, val](arrow::ArrayBuilder *ptr) {
            auto builder = dynamic_cast<arrow::StructBuilder *>(ptr);
            ARROW_RETURN_NOT_OK(builder->Append());

            auto scale_builder = dynamic_cast<arrow::Int32Builder *>(builder->child(0));
            auto val_builder = dynamic_cast<arrow::BinaryBuilder *>(builder->child(1));
            ARROW_RETURN_NOT_OK(scale_builder->Append(scale));
            return val_builder->Append(val);
        };
        ARROW_RETURN_NOT_OK(append_decimal(builder_ptr));
        if (second_ptr)
            ARROW_RETURN_NOT_OK(append_decimal(second_ptr));
        return arrow::Status::OK();
    }
    else if (coltype == conversions::types::DurationType) // duration
    {
        auto months{vint_t(&ks).val()}, days{vint_t(&ks).val()}, nanos{vint_t(&ks).val()};
        auto append_duration = [months, days, nanos](arrow::ArrayBuilder *ptr) {
            auto builder = dynamic_cast<arrow::FixedSizeListBuilder *>(ptr);
            auto value_builder = dynamic_cast<arrow::Int64Builder *>(builder->value_builder());
            ARROW_RETURN_NOT_OK(builder->Append());
            ARROW_RETURN_NOT_OK(value_builder->Append(months));
            ARROW_RETURN_NOT_OK(value_builder->Append(days));
            ARROW_RETURN_NOT_OK(value_builder->Append(nanos));
            return arrow::Status::OK();
        };
        ARROW_RETURN_NOT_OK(append_duration(builder_ptr));
        if (second_ptr)
            ARROW_RETURN_NOT_OK(append_duration(second_ptr));
        return arrow::Status::OK();
    }
    else if (coltype == conversions::types::InetAddressType) // inet
    {
        auto size = ks.size();
        if (size != 4 && size != 8)
            return arrow::Status::TypeError("invalid IP address of size " + std::to_string(ks.size()) +
                                            "bytes. needs to be 4 or 8");
        auto val = size == 4 ? ks.read_s4be() : ks.read_s8be();
        auto append_inet_addr = [size, val](arrow::ArrayBuilder *ptr) {
            auto builder = dynamic_cast<arrow::DenseUnionBuilder *>(ptr);
            if (size == 4)
            {
                ARROW_RETURN_NOT_OK(builder->Append(0));
                auto ipv4_builder = dynamic_cast<arrow::Int32Builder *>(builder->child(0));
                return ipv4_builder->Append(val);
            }
            else
            {
                ARROW_RETURN_NOT_OK(builder->Append(1));
                auto ipv6_builder = dynamic_cast<arrow::Int64Builder *>(builder->child(1));
                return ipv6_builder->Append(val);
            }
        };
        ARROW_RETURN_NOT_OK(append_inet_addr(builder_ptr));
        if (second_ptr)
            ARROW_RETURN_NOT_OK(append_inet_addr(second_ptr));
        return arrow::Status::OK();
    }
    else if (coltype == conversions::types::IntegerType) // varint
    {
        auto val = vint_t::parse_java(bytes.data(), bytes.size());
        auto builder = dynamic_cast<arrow::Int64Builder *>(builder_ptr);
        ARROW_RETURN_NOT_OK(builder->Append(val));
        if (second_ptr)
        {
            auto second = dynamic_cast<arrow::Int64Builder *>(second_ptr);
            ARROW_RETURN_NOT_OK(second->Append(val));
        }
        return arrow::Status::OK();
    }
    else if (coltype == conversions::types::SimpleDateType) // date
    {
        auto builder = dynamic_cast<arrow::Date32Builder *>(builder_ptr);
        uint32_t date = ks.read_u4be() - (1 << 31); // algorithm from cassandra
        return builder->Append(date);
    }
    else if (conversions::is_uuid(coltype))
    {
        if (second_ptr != nullptr)
        {
            auto first_builder = dynamic_cast<arrow::UInt64Builder *>(builder_ptr);
            auto second_builder = dynamic_cast<arrow::UInt64Builder *>(second_ptr);
            auto ks = kaitai::kstream(std::string(bytes));
            ARROW_RETURN_NOT_OK(first_builder->Append(ks.read_u8be()));
            ARROW_RETURN_NOT_OK(second_builder->Append(ks.read_u8be()));
        }
        else
        {
            auto builder = dynamic_cast<arrow::FixedSizeBinaryBuilder *>(builder_ptr);
            ARROW_RETURN_NOT_OK(builder->Append(ks.read_bytes(16)));
        }
        return arrow::Status::OK();
    }

    // if the current type is not cudf supported, it should have been
    // treated as a string by conversions::get_arrow_type,
    // so the builder here should also be a string builder,
    // to which we append the raw bytes as hexadecimal
    try
    {
        conversions::cassandra_type _t = conversions::type_info.at(coltype);
        if (global_flags.for_cudf && !_t.cudf_supported)
        {
            auto builder = dynamic_cast<arrow::StringBuilder *>(builder_ptr);
            return builder->Append(boost::algorithm::hex(std::string(bytes)));
        }
        // otherwise continue on below and append as normal
    }
    catch (const std::out_of_range &err)
    {
        // pass if we don't find the type
    }

    // handle "primitive" types with a macro
    //std::cout << "coltype: " << coltype << "\n";
    //    std::cout << "value: " << val << "\n";  //backslash 
#define APPEND_TO_BUILDER(cassandra_type, arrow_type, read_size)                                                       \
    else if (coltype == conversions::types::cassandra_type##Type)                                                      \
    {                                                                                                                  \
        if (ks.is_eof()){                                                                                                \
            return arrow::Status::OK();                                                                                \
        }                                                                                                              \
        auto val = ks.read_##read_size();                                                                              \
        auto builder = dynamic_cast<arrow::arrow_type##Builder *>(builder_ptr);                                        \
        ARROW_RETURN_NOT_OK(builder->Append(val));                                                                     \
        if (second_ptr)                                                                                                \
            ARROW_RETURN_NOT_OK(dynamic_cast<arrow::arrow_type##Builder *>(second_ptr)->Append(val));                  \
        return arrow::Status::OK();                                                                                    \
    }

    // the macro expands to an "else if" so we need this
    // we also ignore the uuid types handled above
    if (false)
    {
    }
    APPEND_TO_BUILDER(Ascii, LargeString, bytes_full)
    APPEND_TO_BUILDER(Boolean, Boolean, u1)
    APPEND_TO_BUILDER(Byte, Int8, s1)
    APPEND_TO_BUILDER(Bytes, Binary, bytes_full)
    APPEND_TO_BUILDER(Double, Double, f8be)
    APPEND_TO_BUILDER(Float, Float, f4be)
    APPEND_TO_BUILDER(Int32, Int32, s4be)
    APPEND_TO_BUILDER(Long, Int64, s8be)
    APPEND_TO_BUILDER(Short, Int16, s2be)
    APPEND_TO_BUILDER(Time, Time64, s8be)
    APPEND_TO_BUILDER(Timestamp, Timestamp, s8be)
    APPEND_TO_BUILDER(UTF8, LargeString, bytes_full)

#undef APPEND_TO_BUILDER

    return arrow::Status::TypeError(std::string("unrecognized type when appending to arrow array builder: ") +
                                    std::string(coltype));
}
} // namespace

arrow::Status append_ts_if_exists(column_t::ts_builder_t *builder, const std::unique_ptr<conversion_helper_t> &helper,
                                  streaming_sstable_data_t::simple_cell_t *cell)
{
    if (cell->_is_null_delta_timestamp())
        return builder->AppendNull();
    else
    {
        uint64_t delta = cell->delta_timestamp()->val();
        return builder->Append(helper->get_timestamp(delta));
    }
}
arrow::Status append_local_del_time_if_exists(column_t::local_del_time_builder_t *builder,
                                              const std::unique_ptr<conversion_helper_t> &helper,
                                              streaming_sstable_data_t::simple_cell_t *cell)
{
    if (cell->_is_null_delta_local_deletion_time())
        return builder->AppendNull();
    else
    {
        uint64_t delta = cell->delta_local_deletion_time()->val();
        return builder->Append(helper->get_local_del_time(delta));
    }
}
arrow::Status append_ttl_if_exists(column_t::ttl_builder_t *builder, const std::unique_ptr<conversion_helper_t> &helper,
                                   streaming_sstable_data_t::simple_cell_t *cell)
{
    if (cell->_is_null_delta_ttl())
        return builder->AppendNull();
    else
    {
        uint64_t delta = cell->delta_ttl()->val();
        return builder->Append(helper->get_ttl(delta));
    }
}

// idx is the index of the desired column in the superset of columns contained
// in this SSTable
bool does_cell_exist(streaming_sstable_data_t::row_t *row, const uint64_t &idx)
{
    return row->_is_null_columns_bitmask() || (row->columns_bitmask()->bitmask & (1U << idx)) == 0;
}

} // namespace sstable_to_arrow
