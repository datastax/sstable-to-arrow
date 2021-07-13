// See http://arrow.apache.org/docs/cpp/examples/row_columnar_conversion.html

#include "sstable_to_arrow.h"

typedef std::shared_ptr<std::vector<std::string>> str_arr_t;
typedef std::shared_ptr<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> builder_arr_t;
typedef std::shared_ptr<std::vector<std::shared_ptr<arrow::DataType>>> data_type_arr_t;

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
    PROFILE_FUNCTION;
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
    PROFILE_FUNCTION;

    auto start_ts = std::chrono::high_resolution_clock::now();
    auto start = std::chrono::time_point_cast<std::chrono::microseconds>(start_ts).time_since_epoch().count();

    arrow::MemoryPool *pool = arrow::default_memory_pool();

    auto &serialization_ptr = (*statistics->toc()->array())[3];
    auto serialization_header = dynamic_cast<sstable_statistics_t::serialization_header_t *>(serialization_ptr->body());
    assert(serialization_header != nullptr);

    str_arr_t types = std::make_shared<std::vector<std::string>>();
    str_arr_t names = std::make_shared<std::vector<std::string>>();
    builder_arr_t arr = std::make_shared<std::vector<std::shared_ptr<arrow::ArrayBuilder>>>();
    data_type_arr_t data_types = std::make_shared<std::vector<std::shared_ptr<arrow::DataType>>>();

    DEBUG_ONLY(std::cout << "saving partition key\n");

    // use process_column to create a vector to contain row timestamps
    types->push_back("org.apache.cassandra.db.marshal.TimestampType");
    names->push_back("_timestamp");
    arr->push_back(std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::MICRO), pool));
    data_types->push_back(arrow::timestamp(arrow::TimeUnit::MICRO));

    std::string partition_key_type = serialization_header->partition_key_type()->body();
    process_column(types, names, arr, data_types, partition_key_type, "partition key", pool);

    DEBUG_ONLY(std::cout << "saving clustering keys\n");
    for (auto &col : *serialization_header->clustering_key_types()->array())
        process_column(types, names, arr, data_types, col->body(), "clustering key", pool);

    // TODO handle static columns
    DEBUG_ONLY(std::cout << "saving regular columns\n");
    for (auto &col : *serialization_header->regular_columns()->array())
        process_column(types, names, arr, data_types, col->column_type()->body(), col->name()->body(), pool);

    for (auto &partition : *sstable->partitions())
        for (auto &unfiltered : *partition->unfiltereds())
            if ((unfiltered->flags() & 0x01) == 0 && (unfiltered->flags() & 0x02) == 0) // ensure that this is a row instead of an end of partition marker or range tombstone marker
                                                                                        // TODO handle end of partition and range tombstone markers
                process_row(partition->header()->key(), unfiltered, types, names, arr, serialization_header, pool);

    int n = arr->size();
    DEBUG_ONLY(std::cout << "number of fields in table: " << n << '\n');

    // finish the arrays and store them into a vector

    arrow::ArrayVector finished_arrays;
    arrow::FieldVector schema_vector;
    for (int i = 0; i < n; ++i)
    {
        std::shared_ptr<arrow::Array> arrptr;
        ARROW_RETURN_NOT_OK((*arr)[i]->Finish(&arrptr));
        finished_arrays.push_back(arrptr);
        schema_vector.push_back(arrow::field(std::string((*names)[i]), (*data_types)[i]));
    }
    *schema = std::make_shared<arrow::Schema>(schema_vector);
    *table = arrow::Table::Make(*schema, finished_arrays);

    std::cout
        << "==========\nschema:\n==========\n"
        << (*schema)->ToString()
        << "\n==========\ntable:\n==========\n"
        << (*table)->ToString() << "\n==========\n";

    auto end_ts = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::time_point_cast<std::chrono::microseconds>(end_ts).time_since_epoch().count();

    std::cout << "[PROFILE conversion]: " << (end - start) << "us\n";

    return arrow::Status::OK();
}

void process_column(
    str_arr_t types,
    str_arr_t names,
    builder_arr_t arr,
    data_type_arr_t data_types,
    const std::string &cassandra_type,
    const std::string &name,
    const std::shared_ptr<arrow::DataType> &data_type,
    arrow::MemoryPool *pool)
{
    PROFILE_FUNCTION;
    DEBUG_ONLY(std::cout << "Handling column \"" << name << "\" with type " << cassandra_type << '\n');
    types->push_back(cassandra_type);
    names->push_back(name);
    arr->push_back(create_builder(cassandra_type, pool));
    data_types->push_back(data_type);
}

/**
 * @brief creates an Arrow ArrayBuilder based on the column type and name and adds it to an array of ArrayBuilders.
 * 
 * @param types a vector of type names
 * @param names a vector of column names in the table
 * @param arr the array of Arrow ArrayBuilders
 * @param cassandra_type the CQL type of the column as a string
 * @param name the name of the column
 */
void process_column(
    str_arr_t types,
    str_arr_t names,
    builder_arr_t arr,
    data_type_arr_t data_types,
    const std::string &cassandra_type,
    const std::string &name,
    arrow::MemoryPool *pool)
{
    process_column(types, names, arr, data_types, cassandra_type, name, conversions::get_arrow_type(cassandra_type), pool);
}

arrow::Status process_row(
    std::string_view partition_key,
    std::unique_ptr<sstable_data_t::unfiltered_t> &unfiltered,
    str_arr_t types,
    str_arr_t names,
    builder_arr_t arr,
    sstable_statistics_t::serialization_header_t *serialization_header,
    arrow::MemoryPool *pool)
{
    PROFILE_FUNCTION;

    // now we know that this unfiltered is actually a row
    sstable_data_t::row_t *row = static_cast<sstable_data_t::row_t *>(unfiltered->body());

    // counter for which index in the global builders array we are in
    int idx = 0;

    arrow::TimestampBuilder *builder = (arrow::TimestampBuilder *)(*arr)[idx].get();
    if (!row->_is_null_liveness_info())
    {
        long long delta_timestamp = row->liveness_info()->delta_timestamp()->val();
        builder->Append(serialization_header->min_timestamp()->val() + delta_timestamp);
    }
    else
        builder->AppendNull();
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
        ARROW_RETURN_NOT_OK(append_scalar(
            coltype,
            builder_ptr,
            (sstable_data_t::complex_cell_t *)cell_ptr.get(),
            pool));
    }
    else // simple cell
    {
        ARROW_RETURN_NOT_OK(
            append_scalar(
                coltype,
                builder_ptr,
                ((sstable_data_t::simple_cell_t *)cell_ptr.get())->value(),
                pool));
    }
    return arrow::Status::OK();
}

/**
 * @brief See https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/marshal/TypeParser.java
 * @param types the vector of the Cassandra types of each column (e.g. org.apache.cassandra.db.marshal.AsciiType)
 * @param names the vector of the names of each column
 * @param cassandra_type the Cassandra type of this column (not the actual CQL type name)
 * @param name the name of this column
 * @param pool the arrow memory pool to use for the array builders
 */
std::shared_ptr<arrow::ArrayBuilder> create_builder(const std::string_view &type, arrow::MemoryPool *pool)
{
    PROFILE_FUNCTION;
    DEBUG_ONLY(std::cout << "creating new vector of type " << type << '\n');
    if (type == "org.apache.cassandra.db.marshal.AsciiType") // ascii
        return std::make_shared<arrow::StringBuilder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.BooleanType") // boolean
        return std::make_shared<arrow::BooleanBuilder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.ByteType") // tinyint
        return std::make_shared<arrow::Int8Builder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.BytesType") // blob
        return std::make_shared<arrow::BinaryBuilder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.DecimalType") // decimal
    {
        arrow::FieldVector fields;
        fields.push_back(arrow::field("scale", arrow::int32()));
        fields.push_back(arrow::field("value", arrow::binary()));
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
        field_builders.push_back(std::make_shared<arrow::Int32Builder>(pool));
        field_builders.push_back(std::make_shared<arrow::BinaryBuilder>(pool));
        return std::make_shared<arrow::StructBuilder>(arrow::struct_(fields), pool, field_builders);
    }
    else if (type == "org.apache.cassandra.db.marshal.DoubleType") // double
        return std::make_shared<arrow::DoubleBuilder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.DurationType") // duration, consists of two int32s and one int64
    {
        auto val_builder = std::make_shared<arrow::Int64Builder>(pool);
        return std::make_shared<arrow::FixedSizeListBuilder>(pool, val_builder, 3);
    }
    else if (type == "org.apache.cassandra.db.marshal.FloatType") // float
        return std::make_shared<arrow::FloatBuilder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.InetAddressType") // inet
    {
        auto union_builder = std::make_shared<arrow::DenseUnionBuilder>(pool);
        auto ipv4_builder = std::make_shared<arrow::Int32Builder>(pool);
        auto ipv6_builder = std::make_shared<arrow::Int64Builder>(pool);
        union_builder->AppendChild(ipv4_builder, "ipv4");
        union_builder->AppendChild(ipv6_builder, "ipv6");
        return union_builder;
    }
    else if (type == "org.apache.cassandra.db.marshal.Int32Type") // int
        return std::make_shared<arrow::Int32Builder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.IntegerType") // varint
        return std::make_shared<arrow::Int64Builder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.LongType") // bigint
        return std::make_shared<arrow::Int64Builder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.ShortType") // smallint
        return std::make_shared<arrow::Int16Builder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.SimpleDateType") // date
        return std::make_shared<arrow::Date32Builder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.TimeType") // time
        return std::make_shared<arrow::Time64Builder>(arrow::time64(arrow::TimeUnit::NANO), pool);
    else if (type == "org.apache.cassandra.db.marshal.TimeUUIDType") // timeuuid
        return std::make_shared<arrow::FixedSizeBinaryBuilder>(arrow::fixed_size_binary(16), pool);
    else if (type == "org.apache.cassandra.db.marshal.TimestampType" || type == "org.apache.cassandra.db.marshal.DateType") // timestamp
        return std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::MILLI), pool);
    else if (type == "org.apache.cassandra.db.marshal.UTF8Type") // text, varchar
        return std::make_shared<arrow::StringBuilder>(pool);
    else if (type == "org.apache.cassandra.db.marshal.UUIDType") // uuid
        return std::make_shared<arrow::FixedSizeBinaryBuilder>(arrow::fixed_size_binary(16), pool);

    // TODO other types
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.CollectionType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.ColumnToCollectionType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.CompositeType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.CounterColumnType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.DateType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.DynamicCompositeType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.EmptyType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.FrozenType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.PartitionerDefinedOrder")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.ReversedType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.LexicalUUIDType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.UserType")
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.TupleType")
    //     arr->push_back(std::make_shared<arrow::UInt32Builder>(pool));

    else if (conversions::is_reversed(type))
    {
        return create_builder(conversions::get_child_type(type), pool);
    }
    else if (conversions::is_composite(type))
    {
        arrow::FieldVector fields;
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
        auto maybe_tree = conversions::parse_nested_type(type);
        auto tree = *maybe_tree;
        for (auto field : *tree->children)
        {
            fields.push_back(arrow::field(std::string(field->str), conversions::get_arrow_type(field->str)));
            field_builders.push_back(create_builder(field->str, pool));
        }
        return std::make_shared<arrow::StructBuilder>(arrow::struct_(fields), pool, field_builders);
    }
    else if (conversions::is_list(type)) // list<type>
    {
        return std::make_shared<arrow::ListBuilder>(pool, create_builder(conversions::get_child_type(type), pool));
    }
    else if (conversions::is_map(type)) // if it begins with the map type map<type, type>
    {
        // TODO this currently only works if both the key and value are simple types, i.e. not maps
        std::string_view key_type, value_type;
        conversions::get_map_child_types(type, &key_type, &value_type);
        DEBUG_ONLY(std::cout << "map types: " << key_type << ": " << value_type << '\n');
        auto key_builder = create_builder(key_type, pool);
        auto value_builder = create_builder(value_type, pool);
        return std::make_shared<arrow::MapBuilder>(pool, key_builder, value_builder);
    }
    else if (conversions::is_set(type)) // set<type>
    {
        return std::make_shared<arrow::ListBuilder>(pool, create_builder(conversions::get_child_type(type), pool));
    }
    else
    {
        std::cerr << "unrecognized type when creating arrow array builder: " << type << '\n';
        exit(1);
    }
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
    PROFILE_FUNCTION;
    DEBUG_ONLY(std::cout << "appending to vector: " << coltype << '\n');

    // for all other types, we parse the data using kaitai, which might end up
    // being a performance bottleneck
    // TODO look into potential uses of memcpy for optimization
    std::string buffer(bytes);
    kaitai::kstream ks(buffer);

    // for ascii or blob or varchar or text, we just return the bytes directly
    if (coltype == "org.apache.cassandra.db.marshal.AsciiType" ||
        coltype == "org.apache.cassandra.db.marshal.BytesType" ||
        coltype == "org.apache.cassandra.db.marshal.UTF8Type")
    {
        auto builder = (arrow::StringBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_bytes_full()));
        return arrow::Status::OK();
    }

    if (conversions::is_composite(coltype))
    {
        auto builder = static_cast<arrow::StructBuilder *>(builder_ptr);
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
    }
    else if (conversions::is_reversed(coltype))
    {
        ARROW_RETURN_NOT_OK(append_scalar(conversions::get_child_type(coltype), builder_ptr, bytes, pool));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.BooleanType") // boolean
    {
        auto builder = (arrow::BooleanBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_u1()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.ByteType") // tinyint
    {
        auto builder = (arrow::Int8Builder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_s1()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.DecimalType") // decimal
    {
        auto builder = (arrow::StructBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append());
        auto scale_builder = (arrow::Int32Builder *)builder->child(0);
        auto val_builder = (arrow::BinaryBuilder *)builder->child(1);
        int scale = ks.read_s4be();
        ARROW_RETURN_NOT_OK(scale_builder->Append(scale));
        ARROW_RETURN_NOT_OK(val_builder->Append(ks.read_bytes_full()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.DoubleType") // double
    {
        auto builder = (arrow::DoubleBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_f8be()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.DurationType") // duration
    {
        auto builder = (arrow::FixedSizeListBuilder *)builder_ptr;
        auto value_builder = (arrow::Int64Builder *)builder->value_builder();
        long long months = vint_t(&ks).val();
        long long days = vint_t(&ks).val();
        long long nanoseconds = vint_t(&ks).val();
        ARROW_RETURN_NOT_OK(builder->Append());
        ARROW_RETURN_NOT_OK(value_builder->Append(months));
        ARROW_RETURN_NOT_OK(value_builder->Append(days));
        ARROW_RETURN_NOT_OK(value_builder->Append(nanoseconds));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.FloatType") // float
    {
        auto builder = (arrow::FloatBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_f4be()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.InetAddressType") // inet
    {
        auto builder = (arrow::DenseUnionBuilder *)builder_ptr;
        if (ks.size() == 4)
        {
            builder->Append(0);
            auto ipv4_builder = static_cast<arrow::Int32Builder *>(builder->child(0));
            ARROW_RETURN_NOT_OK(ipv4_builder->Append(ks.read_s4be()));
        }
        else if (ks.size() == 8)
        {
            builder->Append(1);
            auto ipv6_builder = static_cast<arrow::Int64Builder *>(builder->child(1));
            ARROW_RETURN_NOT_OK(ipv6_builder->Append(ks.read_s8be()));
        }
        else
        {
            std::cerr << "invalid IP address of size " << ks.size() << " bytes. needs to be 4 or 8\n";
            return arrow::Status::TypeError("invalid IP address");
        }
    }
    else if (coltype == "org.apache.cassandra.db.marshal.Int32Type") // int
    {
        auto builder = (arrow::Int32Builder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_s4be()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.IntegerType") // varint
    {
        auto builder = static_cast<arrow::Int64Builder *>(builder_ptr);
        int size = ks.size();
        ARROW_RETURN_NOT_OK(builder->Append(vint_t::parse_java(bytes.data(), bytes.size())));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.LongType") // bigint
    {
        auto builder = (arrow::Int64Builder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_s8be()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.ShortType") // smallint
    {
        auto builder = (arrow::Int16Builder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_s2be()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.SimpleDateType") // date
    {
        auto builder = (arrow::Date32Builder *)builder_ptr;
        uint32_t date = ks.read_u4be() - (1 << 31);
        ARROW_RETURN_NOT_OK(builder->Append(date));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.TimeType") // time
    {
        auto builder = (arrow::Time64Builder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_s8be()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.TimeUUIDType") // timeuuid
    {
        auto builder = (arrow::FixedSizeBinaryBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_bytes_full()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.TimestampType") // timestamp
    {
        auto builder = (arrow::TimestampBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_s8be()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.UUIDType") // uuid
    {
        auto builder = (arrow::FixedSizeBinaryBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_bytes_full()));
    }
    else
    {
        std::cerr << "unrecognized type when appending to arrow array builder: " << coltype << '\n';
        exit(1);
    }

    return arrow::Status::OK();
}

arrow::Status append_scalar(const std::string_view &coltype, arrow::ArrayBuilder *builder_ptr, const sstable_data_t::complex_cell_t *cell, arrow::MemoryPool *pool)
{
    if (conversions::is_map(coltype))
    {
        auto builder = static_cast<arrow::MapBuilder *>(builder_ptr);
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
        auto builder = static_cast<arrow::ListBuilder *>(builder_ptr);
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
        auto builder = static_cast<arrow::ListBuilder *>(builder_ptr);
        ARROW_RETURN_NOT_OK(builder->Append());

        for (const auto &simple_cell : *cell->simple_cells())
        {
            DEBUG_ONLY(std::cout << "child value as string: " << simple_cell->value() << '\n');
            ARROW_RETURN_NOT_OK(append_scalar(conversions::get_child_type(coltype), builder->value_builder(), simple_cell->value(), pool));
        }
    }

    return arrow::Status::OK();
}
