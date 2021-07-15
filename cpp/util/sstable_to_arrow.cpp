// See http://arrow.apache.org/docs/cpp/examples/row_columnar_conversion.html

#include "sstable_to_arrow.h"

typedef std::shared_ptr<std::vector<std::string>> str_arr_t;
typedef std::shared_ptr<std::vector<std::unique_ptr<arrow::ArrayBuilder>>> builder_arr_t;

const int PORT = 9143;

/**
 * @brief Send an Arrow Table across a network socket.
 * 
 * @param schema the schema (field types and names) of the table
 * @param table the Arrow Table containing the SSTable data
 * @return arrow::Status 
 */
arrow::Status send_data(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<arrow::Table> table)
{
    int sockfd;
    FAIL_ON_STATUS(sockfd = socket(AF_INET, SOCK_STREAM, 0), "socket failed");
    int option = 1;
    FAIL_ON_STATUS(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option)), "failed setting socket options");
    std::cout << "created socket at file descriptor " << sockfd << '\n';

    struct sockaddr_in serv_addr;
    memset((char *)&serv_addr, 0x00, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(PORT);

    FAIL_ON_STATUS(bind(
                       sockfd,
                       (struct sockaddr *)&serv_addr,
                       sizeof(serv_addr)),
                   "error on binding");

    std::cout << "listening on port " << PORT << '\n';
    listen(sockfd, 5);

    struct sockaddr_in cli_addr;
    socklen_t clilen = sizeof(cli_addr);
    int newsockfd;
    FAIL_ON_STATUS(newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen), "error on accept");

    char buffer[256];
    memset(buffer, 0x00, 256);
    std::cout << "waiting for message\n";
    FAIL_ON_STATUS(read(newsockfd, buffer, 255), "error reading from socket");

    arrow::Result<std::shared_ptr<arrow::io::BufferOutputStream>> maybe_ostream;
    ARROW_RETURN_NOT_OK(maybe_ostream = arrow::io::BufferOutputStream::Create());
    auto ostream = *maybe_ostream;

    DEBUG_ONLY(std::cout << "making stream writer\n");
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>> maybe_writer;
    ARROW_RETURN_NOT_OK(maybe_writer = arrow::ipc::MakeStreamWriter(ostream, schema));
    auto writer = *maybe_writer;

    ARROW_RETURN_NOT_OK(writer->WriteTable(*table, -1));
    DEBUG_ONLY(
        std::cout << "writer stats:"
                  << "\n\tnum dictionary batches: " << writer->stats().num_dictionary_batches
                  << "\n\tnum dictionary deltas: " << writer->stats().num_dictionary_deltas
                  << "\n\tnum messages: " << writer->stats().num_messages
                  << "\n\tnum record batches: " << writer->stats().num_record_batches
                  << "\n\tnum replaced dictionaries: " << writer->stats().num_replaced_dictionaries
                  << '\n');
    ARROW_RETURN_NOT_OK(writer->Close());

    DEBUG_ONLY(std::cout << "finishing stream\n");
    arrow::Result<std::shared_ptr<arrow::Buffer>> maybe_bytes;
    ARROW_RETURN_NOT_OK(maybe_bytes = ostream->Finish());
    auto bytes = *maybe_bytes;

    DEBUG_ONLY(std::cout << "buffer size (number of bytes written): " << bytes->size() << '\n');

    FAIL_ON_STATUS(write(newsockfd, (char *)bytes->data(), bytes->size()), "error writing to socket");

    DEBUG_ONLY(std::cout << "closing sockets\n");

    close(newsockfd);
    close(sockfd);

    DEBUG_ONLY(std::cout << "closed sockets\n");

    return arrow::Status::OK();
}

arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Schema> *schema, std::shared_ptr<arrow::Table> *table)
{
    auto start_ts = std::chrono::high_resolution_clock::now();
    auto start = std::chrono::time_point_cast<std::chrono::microseconds>(start_ts).time_since_epoch().count();

    arrow::MemoryPool *pool = arrow::default_memory_pool();

    auto &statistics_ptr = (*statistics->toc()->array())[2];
    auto statistics_data = dynamic_cast<sstable_statistics_t::statistics_t *>(statistics_ptr->body());
    assert(statistics_data != nullptr);

    auto serialization_header = get_serialization_header(statistics);

    std::cout << "min timestamp: " << serialization_header->min_timestamp()->val() << '\n';

    str_arr_t types = std::make_shared<std::vector<std::string>>();                            // cql types of all columns
    arrow::FieldVector schema_vector;                                                          // name and arrow datatype of all columns
    builder_arr_t arr = std::make_shared<std::vector<std::unique_ptr<arrow::ArrayBuilder>>>(); // arrow builder for each column

    DEBUG_ONLY(std::cout << "saving partition key\n");

    int64_t nrows = statistics_data->number_of_rows();
    std::cout << "nrows: " << nrows << '\n';

    // use create a vector to contain row timestamps
    process_column(types, schema_vector, arr, "org.apache.cassandra.db.marshal.TimestampType", "liveness_info_tstamp", arrow::timestamp(arrow::TimeUnit::MICRO), nrows);

    std::string partition_key_type = serialization_header->partition_key_type()->body();
    process_column(types, schema_vector, arr, partition_key_type, "partition key", conversions::get_arrow_type(partition_key_type), nrows);

    DEBUG_ONLY(std::cout << "saving clustering keys\n");
    for (auto &col : *serialization_header->clustering_key_types()->array())
        process_column(types, schema_vector, arr, col->body(), "clustering key", conversions::get_arrow_type(col->body()), nrows);

    // TODO handle static columns
    DEBUG_ONLY(std::cout << "saving regular columns\n");
    for (auto &col : *serialization_header->regular_columns()->array())
        process_column(types, schema_vector, arr, col->column_type()->body(), col->name()->body(), conversions::get_arrow_type(col->column_type()->body()), nrows);

    *schema = std::make_shared<arrow::Schema>(schema_vector);
    std::cout << "========== schema ==========\n"
              << (*schema)->ToString() << "\n==========\n";

    for (auto &builder : *arr)
        ARROW_RETURN_NOT_OK(reserve_builder(builder.get(), nrows));

    for (auto &partition : *sstable->partitions())
        for (auto &unfiltered : *partition->unfiltereds())
            if ((unfiltered->flags() & 0x01) == 0 && (unfiltered->flags() & 0x02) == 0) // ensure that this is a row instead of an end of partition marker or range tombstone marker
                                                                                        // TODO handle end of partition and range tombstone markers
                process_row(partition->header()->key(), unfiltered, types, arr, serialization_header, pool);

    int n = arr->size();
    DEBUG_ONLY(std::cout << "number of fields in table: " << n << '\n');

    // finish the arrays and store them into a vector

    arrow::ArrayVector finished_arrays;
    for (auto &builder : *arr)
    {
        std::shared_ptr<arrow::Array> arrptr;
        ARROW_RETURN_NOT_OK(builder->Finish(&arrptr));
        finished_arrays.push_back(arrptr);
    }
    *table = arrow::Table::Make(*schema, finished_arrays);

    std::cout << "\n========== table ==========\n"
              << (*table)->ToString() << "\n==========\n";

    auto end_ts = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::time_point_cast<std::chrono::microseconds>(end_ts).time_since_epoch().count();

    std::cout << "[PROFILE conversion]: " << (end - start) << "us\n";

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

arrow::Status process_column(
    str_arr_t types,
    arrow::FieldVector &schema_vector,
    builder_arr_t arr,
    const std::string &cassandra_type,
    const std::string &name,
    const std::shared_ptr<arrow::DataType> &data_type,
    int64_t nrows,
    arrow::MemoryPool *pool)
{
    DEBUG_ONLY(std::cout << "Handling column \"" << name << "\" with type " << cassandra_type << '\n');
    types->push_back(cassandra_type);
    schema_vector.push_back(arrow::field(name, data_type));
    std::unique_ptr<arrow::ArrayBuilder> builder;
    ARROW_RETURN_NOT_OK(arrow::MakeBuilder(pool, data_type, &builder));
    arr->push_back(std::move(builder));
    return arrow::Status::OK();
}

arrow::Status process_row(
    std::string_view partition_key,
    std::unique_ptr<sstable_data_t::unfiltered_t> &unfiltered,
    str_arr_t types,
    builder_arr_t arr,
    sstable_statistics_t::serialization_header_t *serialization_header,
    arrow::MemoryPool *pool)
{
    // now we know that this unfiltered is actually a row
    sstable_data_t::row_t *row = static_cast<sstable_data_t::row_t *>(unfiltered->body());

    // counter for which index in the global builders array we are in
    int idx = 0;

    auto builder = dynamic_cast<arrow::TimestampBuilder *>((*arr)[idx].get());
    if (!row->_is_null_liveness_info())
    {
        uint64_t delta_timestamp = row->liveness_info()->delta_timestamp()->val();
        std::cout << "delta: " << delta_timestamp << ", " << serialization_header->min_timestamp()->val() + delta_timestamp + deserialization_helper_t::TIMESTAMP_EPOCH << '\n';
        builder->UnsafeAppend(serialization_header->min_timestamp()->val() + delta_timestamp + deserialization_helper_t::TIMESTAMP_EPOCH);
    }
    else
        builder->UnsafeAppendNull();
    idx++;

    int kind = ((unfiltered->flags() & 0x80) != 0) && ((row->extended_flags() & 0x01) != 0)
                   ? deserialization_helper_t::STATIC
                   : deserialization_helper_t::REGULAR; // TODO deal with static row

    ARROW_RETURN_NOT_OK(append_scalar((*types)[idx], (*arr)[idx].get(), partition_key, pool));
    idx++;
    for (auto &cell : *row->clustering_blocks()->values())
    {
        ARROW_RETURN_NOT_OK(append_scalar((*types)[idx], (*arr)[idx].get(), cell, pool));
        idx++;
    }

    const int &n_regular = deserialization_helper_t::get_n_cols(deserialization_helper_t::REGULAR);
    std::vector<std::future<arrow::Status>> col_threads;
    col_threads.reserve(n_regular);

    // parse each of the row's cells
    for (int i = 0; i < n_regular; ++i, ++idx)
    {
        auto cell_ptr = std::move((*row->cells())[i]);
        auto builder_ptr = (*arr)[idx].get();
        const std::string_view &coltype = (*types)[idx];
        const bool is_multi_cell = deserialization_helper_t::is_multi_cell(deserialization_helper_t::REGULAR, i);
        col_threads.push_back(std::async(handle_cell, std::move(cell_ptr), builder_ptr, coltype, is_multi_cell, pool));
    }

    for (auto &future : col_threads)
        ARROW_RETURN_NOT_OK(future.get());

    return arrow::Status::OK();
}

arrow::Status handle_cell(std::unique_ptr<kaitai::kstruct> cell_ptr, arrow::ArrayBuilder *builder_ptr, const std::string_view &coltype, bool is_multi_cell, arrow::MemoryPool *pool)
{
    if (is_multi_cell)
    {
        ARROW_RETURN_NOT_OK(append_complex(
            coltype,
            builder_ptr,
            dynamic_cast<sstable_data_t::complex_cell_t *>(cell_ptr.get()),
            pool));
    }
    else // simple cell
    {
        ARROW_RETURN_NOT_OK(append_scalar(
            coltype,
            builder_ptr,
            dynamic_cast<sstable_data_t::simple_cell_t *>(cell_ptr.get())->value(),
            pool));
    }
    return arrow::Status::OK();
}

/**
 * @brief Appends a scalar value to an Arrow ArrayBuilder corresponding to a certain CQL type given by `coltype`.
 * 
 * @param coltype the CQL data type of the column
 * @param builder_ptr a pointer to the arrow ArrayBuilder
 * @param bytes a buffer containing the bytes from the SSTable
 */
arrow::Status append_scalar(const std::string_view &coltype, arrow::ArrayBuilder *builder_ptr, const std::string_view &bytes, arrow::MemoryPool *pool)
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
            ks.read_bytes(1); // inserts a '0' bit at end
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

/**
 * @brief Overloaded function to handle adding complex types
 * 
 * @param coltype 
 * @param builder_ptr 
 * @param cell 
 * @param pool 
 * @return arrow::Status 
 */
arrow::Status append_complex(const std::string_view &coltype, arrow::ArrayBuilder *builder_ptr, const sstable_data_t::complex_cell_t *cell, arrow::MemoryPool *pool)
{
    if (conversions::is_map(coltype))
    {
        auto builder = dynamic_cast<arrow::MapBuilder *>(builder_ptr);
        ARROW_RETURN_NOT_OK(builder->Append());
        std::string_view key_type, value_type;
        for (const auto &simple_cell : *cell->simple_cells())
        {
            // keys are stored in the cell path
            conversions::get_map_child_types(coltype, &key_type, &value_type);
            append_scalar(key_type, builder->key_builder(), simple_cell->path()->value(), pool);
            append_scalar(value_type, builder->item_builder(), simple_cell->value(), pool);
            DEBUG_ONLY(std::cout << "key and value as strings: " << simple_cell->path()->value() << " | " << simple_cell->value() << '\n');
        }
    }
    else if (conversions::is_set(coltype))
    {
        auto builder = dynamic_cast<arrow::ListBuilder *>(builder_ptr);
        ARROW_RETURN_NOT_OK(builder->Append());

        for (const auto &simple_cell : *cell->simple_cells())
        {
            // values of a set are stored in the path, while the actual cell value is empty
            DEBUG_ONLY(std::cout << "child value as string: " << simple_cell->path()->value() << '\n');
            ARROW_RETURN_NOT_OK(append_scalar(conversions::get_child_type(coltype), builder->value_builder(), simple_cell->path()->value(), pool));
        }
    }
    else if (conversions::is_list(coltype))
    {
        auto builder = dynamic_cast<arrow::ListBuilder *>(builder_ptr);
        ARROW_RETURN_NOT_OK(builder->Append());

        for (const auto &simple_cell : *cell->simple_cells())
        {
            DEBUG_ONLY(std::cout << "child value as string: " << simple_cell->value() << '\n');
            ARROW_RETURN_NOT_OK(append_scalar(conversions::get_child_type(coltype), builder->value_builder(), simple_cell->value(), pool));
        }
    }

    return arrow::Status::OK();
}

arrow::Status write_parquet(const arrow::Table &table, arrow::MemoryPool *pool)
{
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open("table.parquet"));
    return parquet::arrow::WriteTable(table, pool, outfile, 3);
}

// Read the serialization header from the statistics file.
sstable_statistics_t::serialization_header_t *get_serialization_header(std::shared_ptr<sstable_statistics_t> statistics)
{
    const auto &toc = *statistics->toc()->array();
    const auto &ptr = toc[3]; // 3 is the index of the serialization header in the table of contents in the statistics file
    return dynamic_cast<sstable_statistics_t::serialization_header_t *>(ptr->body());
}
