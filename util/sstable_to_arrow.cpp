// See http://arrow.apache.org/docs/cpp/examples/row_columnar_conversion.html

#include "sstable_to_arrow.h"

typedef std::shared_ptr<std::vector<std::string>> str_arr_t;
typedef std::shared_ptr<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> builder_arr_t;

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
    arrow::MemoryPool *pool = arrow::default_memory_pool();

    auto &ptr = (*statistics->toc()->array())[3];
    auto body = static_cast<sstable_statistics_t::serialization_header_t *>(ptr->body());

    str_arr_t types = std::make_shared<std::vector<std::string>>();
    str_arr_t names = std::make_shared<std::vector<std::string>>();
    builder_arr_t arr = std::make_shared<std::vector<std::shared_ptr<arrow::ArrayBuilder>>>();

    DEBUG_ONLY(std::cout << "saving partition key\n");
    process_column(types, names, arr, body->partition_key_type()->body(), "partition key", pool);

    DEBUG_ONLY(std::cout << "saving clustering keys\n");
    for (auto &col : *body->clustering_key_types()->array())
        process_column(types, names, arr, col->body(), "clustering key", pool);

    // TODO handle static columns
    DEBUG_ONLY(std::cout << "saving regular columns\n");
    for (auto &col : *body->regular_columns()->array())
        process_column(types, names, arr, col->column_type()->body(), col->name()->body(), pool);

    for (auto &partition : *sstable->partitions())
        for (auto &unfiltered : *partition->unfiltereds())
            if ((unfiltered->flags() & 0x01) == 0 && (unfiltered->flags() & 0x02) == 0) // ensure that this is a row instead of an end of partition marker or range tombstone marker
                // TODO handle end of partition and range tombstone markers
                process_row(partition->header()->key(), unfiltered, types, names, arr, pool);

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
        schema_vector.push_back(arrow::field((*names)[i], get_arrow_type((*types)[i])));
    }
    *schema = std::make_shared<arrow::Schema>(schema_vector);
    *table = arrow::Table::Make(*schema, finished_arrays);

    DEBUG_ONLY(
        std::cout
        << "==========\nschema:\n==========\n"
        << (*schema)->ToString()
        << "\n==========\ntable:\n==========\n"
        << (*table)->ToString() << "\n==========\n");

    return arrow::Status::OK();
}

arrow::Status process_row(
    const std::string &partition_key,
    std::unique_ptr<sstable_data_t::unfiltered_t> &unfiltered,
    str_arr_t types,
    str_arr_t names,
    builder_arr_t arr,
    arrow::MemoryPool *pool)
{
    PROFILE_FUNCTION;

    // now we know that this unfiltered is actually a row
    sstable_data_t::row_t *row = static_cast<sstable_data_t::row_t *>(unfiltered->body());

    int idx = 0;
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

    for (int i = 0; i < deserialization_helper_t::get_n_cols(deserialization_helper_t::REGULAR); ++i, ++idx)
    {
        auto cell_ptr = (*row->cells())[i].get();
        auto builder_ptr = (*arr)[idx].get();
        const std::string &coltype = (*types)[idx];
        if (deserialization_helper_t::is_multi_cell(deserialization_helper_t::REGULAR, i))
            ARROW_RETURN_NOT_OK(append_scalar(
                coltype,
                builder_ptr,
                static_cast<sstable_data_t::complex_cell_t *>(cell_ptr),
                pool));
        else
            ARROW_RETURN_NOT_OK(append_scalar(
                coltype,
                builder_ptr,
                static_cast<sstable_data_t::simple_cell_t *>(cell_ptr)->value(),
                pool));
    }

    return arrow::Status::OK();
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
    const std::string &cassandra_type,
    const std::string &name,
    arrow::MemoryPool *pool)
{
    PROFILE_FUNCTION;
    DEBUG_ONLY(std::cout << "Handling column \"" << name << "\" with type " << cassandra_type << '\n');
    types->push_back(cassandra_type);
    names->push_back(name);
    arr->push_back(create_builder(cassandra_type, pool));
}

/**
 * @brief See https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/marshal/TypeParser.java
 * @param types the vector of the Cassandra types of each column (e.g. org.apache.cassandra.db.marshal.AsciiType)
 * @param names the vector of the names of each column
 * @param cassandra_type the Cassandra type of this column (not the actual CQL type name)
 * @param name the name of this column
 * @param pool the arrow memory pool to use for the array builders
 */
std::shared_ptr<arrow::ArrayBuilder> create_builder(const std::string &type, arrow::MemoryPool *pool)
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
    else if (type.rfind(maptype, 0) == 0) // if it begins with the map type map<type, type>
    {
        // TODO this currently only works if both the key and value are simple types, i.e. not maps
        std::string key_type, value_type;
        get_map_child_types(type, &key_type, &value_type);
        DEBUG_ONLY(std::cout << "map types: " << key_type << ": " << value_type << '\n');
        auto key_builder = create_builder(key_type, pool);
        auto value_builder = create_builder(value_type, pool);
        return std::make_shared<arrow::MapBuilder>(pool, key_builder, value_builder);
    }
    else if (type.rfind(settype, 0) == 0) // set<type>
    {
        return std::make_shared<arrow::ListBuilder>(pool, create_builder(get_child_type(type), pool));
    }
    else if (type.rfind(listtype, 0) == 0) // list<type>
    {
        return std::make_shared<arrow::ListBuilder>(pool, create_builder(get_child_type(type), pool));
    }
    else
    {
        DEBUG_ONLY(std::cout << "unrecognized type when creating arrow array builder: " << type.c_str() << '\n');
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
arrow::Status append_scalar(const std::string &coltype, arrow::ArrayBuilder *builder_ptr, const std::string &bytes, arrow::MemoryPool *pool)
{
    PROFILE_FUNCTION;
    DEBUG_ONLY(std::cout << "appending to vector: " << coltype << '\n');

    // for ascii or blob or varchar or text, we just return the bytes directly
    if (coltype == "org.apache.cassandra.db.marshal.AsciiType" ||
        coltype == "org.apache.cassandra.db.marshal.BytesType" ||
        coltype == "org.apache.cassandra.db.marshal.UTF8Type")
    {
        auto builder = (arrow::StringBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(bytes));
        return arrow::Status::OK();
    }

    // for all other types, we parse the data using kaitai, which might end up
    // being a performance bottleneck
    // TODO look into potential uses of memcpy for optimization
    kaitai::kstream ks(bytes);

    if (coltype == "org.apache.cassandra.db.marshal.BooleanType") // boolean
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
        std::string val = ks.read_bytes_full();
        ARROW_RETURN_NOT_OK(scale_builder->Append(scale));
        ARROW_RETURN_NOT_OK(val_builder->Append(val));
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
        ARROW_RETURN_NOT_OK(builder->Append(vint_t::parse_java(bytes.c_str(), bytes.size())));
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
        ARROW_RETURN_NOT_OK(builder->Append(bytes));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.TimestampType") // timestamp
    {
        auto builder = (arrow::TimestampBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_s8be()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.UUIDType") // uuid
    {
        auto builder = (arrow::FixedSizeBinaryBuilder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(bytes));
    }
    else
    {
        DEBUG_ONLY(std::cout << "unrecognized type when appending to arrow array builder: " << coltype.c_str() << '\n');
        exit(1);
    }

    return arrow::Status::OK();
}

arrow::Status append_scalar(const std::string &coltype, arrow::ArrayBuilder *builder_ptr, const sstable_data_t::complex_cell_t *cell, arrow::MemoryPool *pool)
{
    if (coltype.rfind(maptype, 0) == 0) // if it begins with the map type map<type, type>
    {
        auto builder = static_cast<arrow::MapBuilder *>(builder_ptr);
        ARROW_RETURN_NOT_OK(builder->Append());
        std::string key_type, value_type;
        for (const auto &simple_cell : *cell->simple_cells())
        {
            get_map_child_types(coltype, &key_type, &value_type);
            append_scalar(key_type, builder->key_builder(), simple_cell->path()->value(), pool);
            append_scalar(value_type, builder->item_builder(), simple_cell->value(), pool);
            DEBUG_ONLY(std::cout << "key and value as strings: " << simple_cell->path()->value() << " | " << simple_cell->value() << '\n');
        }
    }
    else if (coltype.rfind(settype, 0) == 0) // set<type>
    {
        auto builder = static_cast<arrow::ListBuilder *>(builder_ptr);
        ARROW_RETURN_NOT_OK(builder->Append());

        for (const auto &simple_cell : *cell->simple_cells())
        {
            DEBUG_ONLY(std::cout << "child value as string: " << simple_cell->path()->value() << '\n');
            ARROW_RETURN_NOT_OK(append_scalar(get_child_type(coltype), builder->value_builder(), simple_cell->path()->value(), pool));
        }
    }
    else if (coltype.rfind(listtype, 0) == 0) // list<type>
    {
        auto builder = static_cast<arrow::ListBuilder *>(builder_ptr);
        ARROW_RETURN_NOT_OK(builder->Append());

        for (const auto &simple_cell : *cell->simple_cells())
        {
            DEBUG_ONLY(std::cout << "child value as string: " << simple_cell->value() << '\n');
            ARROW_RETURN_NOT_OK(append_scalar(get_child_type(coltype), builder->value_builder(), simple_cell->value(), pool));
        }
    }

    return arrow::Status::OK();
}

std::shared_ptr<arrow::DataType> get_arrow_type(const std::string &type)
{
    PROFILE_FUNCTION;
    auto type_ptr = type_info.find(type);
    if (type_ptr != type_info.end())
        return type_ptr->second.arrow_type;
    if (type.rfind(maptype, 0) == 0)
    {
        std::string key_type, value_type;
        get_map_child_types(type, &key_type, &value_type);
        return arrow::map(get_arrow_type(key_type), get_arrow_type(value_type));
    }
    // TODO currently treating sets and lists identically
    else if (type.rfind(settype, 0) == 0 || type.rfind(listtype, 0) == 0)
    {
        return arrow::list(get_arrow_type(get_child_type(type)));
    }

    DEBUG_ONLY(std::cout << "type not found or supported: " << type << '\n');
    exit(1);
}

void get_map_child_types(const std::string &type, std::string *key_type, std::string *value_type)
{
    const int sep_idx = type.find(',');
    *key_type = std::string(type.begin() + maptype.size() + 1, type.begin() + sep_idx);
    *value_type = std::string(type.begin() + sep_idx + 1, type.end() - 1);
}

std::string get_child_type(const std::string &type)
{
    PROFILE_FUNCTION;
    return std::string(
        type.begin() + type.find('(') + 1,
        type.begin() + type.rfind(')'));
}
