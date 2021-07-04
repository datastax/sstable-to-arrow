// See http://arrow.apache.org/docs/cpp/examples/row_columnar_conversion.html

#include "sstable_to_arrow.h"

#define FAIL_ON_STATUS(x, msg) \
    if ((x) < 0)               \
    {                          \
        perror((msg));         \
        exit(1);               \
    }

const int PORT = 9143;

arrow::Status send_data(const std::shared_ptr<arrow::Schema> &schema, const std::shared_ptr<arrow::Table> &table)
{
    int sockfd;
    FAIL_ON_STATUS(sockfd = socket(AF_INET, SOCK_STREAM, 0), "socket failed");

    struct sockaddr_in serv_addr;
    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(PORT);

    FAIL_ON_STATUS(bind(
                       sockfd,
                       (struct sockaddr *)&serv_addr,
                       sizeof(serv_addr)),
                   "error on binding");

    std::cout << "waiting for connection\n";
    listen(sockfd, 5);

    struct sockaddr_in cli_addr;
    socklen_t clilen = sizeof(cli_addr);
    int newsockfd;
    FAIL_ON_STATUS(newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen), "error on accept");

    char buffer[256];
    bzero(buffer, 256);

    std::cout << "listening\n";
    FAIL_ON_STATUS(read(newsockfd, buffer, 255), "error reading from socket");

    auto maybe_ostream = arrow::io::BufferOutputStream::Create();
    ARROW_RETURN_NOT_OK(maybe_ostream);
    auto ostream = *maybe_ostream;

    std::cout << "making stream writer\n";
    auto maybe_writer = arrow::ipc::MakeStreamWriter(ostream, schema);
    ARROW_RETURN_NOT_OK(maybe_writer);
    auto writer = *maybe_writer;

    std::cout << "writing table:\n==========\n"
              << table->ToString() << "\n==========\n";
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table));

    std::cout << "finishing stream\n";
    auto maybe_bytes = ostream->Finish();
    ARROW_RETURN_NOT_OK(maybe_bytes);
    auto bytes = *maybe_bytes;

    std::cout << "buffer size: " << bytes->size();

    FAIL_ON_STATUS(write(newsockfd, (char *)bytes->data(), bytes->size()), "error writing to socket");

    ARROW_RETURN_NOT_OK(writer->Close());

    close(newsockfd);
    close(sockfd);

    return arrow::Status::OK();
}

typedef const std::shared_ptr<std::vector<std::string>> str_arr_t;
typedef const std::shared_ptr<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> builder_arr_t;

const std::string maptype = "org.apache.cassandra.db.marshal.MapType";
const std::string settype = "org.apache.cassandra.db.marshal.SetType";
const std::string listtype = "org.apache.cassandra.db.marshal.ListType";

void append_builder(str_arr_t &types, str_arr_t &names, builder_arr_t &arr, const std::string &cassandra_type, const std::string &name, arrow::MemoryPool *pool)
{
    std::cout << "Handling column \"" << name << "\" with type " << cassandra_type << "\n";
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
    std::cout << "creating new vector of type " << type << '\n';
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
    else if (type == "org.apache.cassandra.db.marshal.TimestampType") // timestamp
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
        const int sep_idx = type.find(',');
        std::string key_type = std::string(type.begin() + maptype.size() + 1, type.begin() + sep_idx);
        std::string value_type = std::string(type.begin() + sep_idx + 1, type.end() - 1);
        std::cout << key_type << ": " << value_type << '\n';
        exit(0);
    }
    else if (type.rfind(settype, 0) == 0) // set<type>
    {
        return std::make_shared<arrow::UInt32Builder>(pool);
    }
    else if (type.rfind(listtype, 0) == 0) // list<type>
    {
        const std::string child_type(
            type.begin() + type.find('(') + 1,
            type.begin() + type.rfind(')'));
        return std::make_shared<arrow::ListBuilder>(pool, create_builder(child_type, pool));
    }
    else
    {
        std::cout << "unrecognized type when creating arrow array builder: " << type.c_str() << '\n';
        exit(1);
    }
}

std::string get_child_type(const std::string &type)
{
    return std::string(
        type.begin() + type.find('(') + 1,
        type.begin() + type.rfind(')'));
}

arrow::Status append_scalar(const std::string &coltype, arrow::ArrayBuilder *builder_ptr, const std::string &bytes, arrow::MemoryPool *pool)
{
    std::cout << "appending to vector: " << coltype << "\n";

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
        int months = ks.read_s4be();
        int days = ks.read_s4be();
        int nanoseconds = ks.read_s8be();
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
        auto builder = (arrow::Int64Builder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_s8be()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.Int32Type") // int
    {
        auto builder = (arrow::Int32Builder *)builder_ptr;
        ARROW_RETURN_NOT_OK(builder->Append(ks.read_s4be()));
    }
    else if (coltype == "org.apache.cassandra.db.marshal.IntegerType") // varint
    {
        if (bytes.size() > 8)
        {
            std::cout << "ERROR: currently only supports up to 8 byte varints\n";
            exit(1);
        }
        auto builder = (arrow::Int64Builder *)builder_ptr;
        int64_t val = 0;
        if (BYTE_ORDER == LITTLE_ENDIAN)
            for (int i = 0; i < bytes.size(); ++i)
                val |= (bytes[bytes.size() - i - 1] & 0xff) << (i * 8);
        else
            memcpy(&val, bytes.c_str(), bytes.size());
        ARROW_RETURN_NOT_OK(builder->Append(val));
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
        uint32_t date = ks.read_u4be() - (1 << 31); // why doesn't this work?
        std::cout << date << " < DATE\n";
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
        std::cout << "unrecognized type when appending to arrow array builder: " << coltype.c_str() << '\n';
        exit(1);
    }

    return arrow::Status::OK();
}

arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Schema> *schema, std::shared_ptr<arrow::Table> *table)
{
    arrow::MemoryPool *pool = arrow::default_memory_pool();

    auto &ptr = (*statistics->toc()->array())[3];
    auto body = (sstable_statistics_t::serialization_header_t *)ptr->body();

    str_arr_t types = std::make_shared<std::vector<std::string>>();
    str_arr_t names = std::make_shared<std::vector<std::string>>();
    builder_arr_t arr = std::make_shared<std::vector<std::shared_ptr<arrow::ArrayBuilder>>>();

    std::cout << "saving partition key\n";
    append_builder(types, names, arr, body->partition_key_type()->body(), "partition key", pool);

    std::cout << "saving clustering keys\n";
    for (auto &col : *body->clustering_key_types()->array())
        append_builder(types, names, arr, col->body(), "clustering key", pool);

    // TODO handle static columns
    std::cout << "saving regular columns\n";
    for (auto &col : *body->regular_columns()->array())
        append_builder(types, names, arr, col->column_type()->body(), col->name()->body(), pool);

    for (std::unique_ptr<sstable_data_t::partition_t> &partition : *sstable->partitions())
    {
        for (std::unique_ptr<sstable_data_t::unfiltered_t> &unfiltered : *partition->unfiltereds())
        {
            if ((unfiltered->flags() & 0x01) != 0) // end of partition
                break;
            if ((unfiltered->flags() & 0x02) != 0) // TODO handle markers
                continue;

            sstable_data_t::row_t *row = (sstable_data_t::row_t *)unfiltered->body();

            int idx = 0;
            int kind = deserialization_helper_t::REGULAR;
            if (((unfiltered->flags() & 0x80) != 0) && ((row->extended_flags() & 0x01) != 0))
                kind = deserialization_helper_t::STATIC;

            ARROW_RETURN_NOT_OK(append_scalar((*types)[idx], (*arr)[idx].get(), partition->header()->key(), pool));
            idx++;
            for (auto &cell : *row->clustering_blocks()->values())
            {
                ARROW_RETURN_NOT_OK(append_scalar((*types)[idx], (*arr)[idx].get(), cell, pool));
                idx++;
            }

            for (int i = 0; i < deserialization_helper_t::get_n_cols(deserialization_helper_t::REGULAR); ++i, ++idx)
            {
                if (deserialization_helper_t::is_complex(deserialization_helper_t::REGULAR, i))
                {
                    sstable_data_t::complex_cell_t *cell = (sstable_data_t::complex_cell_t *)(*row->cells())[i].get();
                    const std::string &coltype = (*types)[idx];
                    // collection types
                    if (coltype.rfind(maptype, 0) == 0) // if it begins with the map type map<type, type>
                    {
                        // TODO this currently only works if both the key and value are simple types, i.e. not maps
                    }
                    // else if (coltype.rfind(settype, 0) == 0) // set<type>
                    // {
                    //     std::make_shared<arrow::UInt32Builder>(pool);
                    // }
                    else if (coltype.rfind(listtype, 0) == 0) // list<type>
                    {
                        auto builder = (arrow::ListBuilder *)(*arr)[idx].get();
                        ARROW_RETURN_NOT_OK(builder->Append());

                        for (const auto &simple_cell : *cell->simple_cells())
                        {
                            std::cout << "child value as string: " << simple_cell->value() << ", num children of builder: " << builder->num_children() << "\n";
                            ARROW_RETURN_NOT_OK(append_scalar(get_child_type(coltype), builder->value_builder(), simple_cell->value(), pool));
                        }
                    }
                }
                else
                {
                    sstable_data_t::simple_cell_t *cell = (sstable_data_t::simple_cell_t *)(*row->cells())[i].get();
                    ARROW_RETURN_NOT_OK(append_scalar((*types)[idx], (*arr)[idx].get(), cell->value(), pool));
                }
            }
        }
    }

    int n = arr->size();
    std::cout << "number of fields in table: " << n << '\n';

    std::vector<std::shared_ptr<arrow::Array>> finished_arrays;
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;

    for (int i = 0; i < n; ++i)
    {
        std::shared_ptr<arrow::Array> arrptr;
        ARROW_RETURN_NOT_OK((*arr)[i]->Finish(&arrptr));
        finished_arrays.push_back(arrptr);
        schema_vector.push_back(arrow::field((*names)[i], get_arrow_type((*types)[i])));
    }

    *schema = std::make_shared<arrow::Schema>(schema_vector);

    *table = arrow::Table::Make(*schema, finished_arrays);

    return arrow::Status::OK();
}

std::shared_ptr<arrow::DataType> get_arrow_type(const std::string &type)
{
    auto type_ptr = type_info.find(type);
    if (type_ptr != type_info.end())
        return type_ptr->second.arrow_type;
    // if (type.rfind(maptype, 0) == 0)

    // if (type.rfind(settype, 0) == 0)
    // {
    //     return arrow::list
    //     get_child_type(type);
    // }
    if (type.rfind(listtype, 0) == 0)
    {
        return arrow::list(get_arrow_type(get_child_type(type)));
    }

    std::cout << "type not found or supported: " << type << '\n';
    exit(1);
}
