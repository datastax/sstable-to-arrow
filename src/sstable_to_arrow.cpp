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

    std::cout << "writing table\n"
              << "rows: " << table->ToString() << "\n";
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

std::map<std::string, std::string> type_map;

// See https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/marshal/TypeParser.java
/**
 * @param types the vector of the Cassandra types of each column (e.g. org.apache.cassandra.db.marshal.AsciiType)
 * @param names the vector of the names of each column
 * @param cassandra_type the Cassandra type of this column (not the actual CQL type name)
 * @param name the name of this column
 * @param pool the arrow memory pool to use for the array builders
 */
void create_builder(str_arr_t &types, str_arr_t &names, builder_arr_t &arr, const std::string cassandra_type, const std::string name, arrow::MemoryPool *pool)
{
    std::cout << cassandra_type << ", " << name << "\n";
    types->push_back(cassandra_type);
    names->push_back(name);

    if (cassandra_type == "org.apache.cassandra.db.marshal.AsciiType") // ascii
        arr->push_back(std::make_shared<arrow::StringBuilder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.BooleanType") // boolean
        arr->push_back(std::make_shared<arrow::BooleanBuilder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.ByteType") // tinyint
        arr->push_back(std::make_shared<arrow::Int8Builder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.BytesType") // blob
        arr->push_back(std::make_shared<arrow::BinaryBuilder>(pool));
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.DecimalType") // decimal
    //     arr->push_back(std::make_shared<arrow::Decimal128Builder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.DoubleType") // double
        arr->push_back(std::make_shared<arrow::DoubleBuilder>(pool));
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.DurationType") // duration
    //     arr->push_back(std::make_shared<arrow::FixedSizeListBuilder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.FloatType") // float
        arr->push_back(std::make_shared<arrow::FloatBuilder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.InetAddressType") // inet
        arr->push_back(std::make_shared<arrow::BinaryBuilder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.Int32Type") // int
        arr->push_back(std::make_shared<arrow::Int32Builder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.IntegerType") // varint
        arr->push_back(std::make_shared<arrow::AdaptiveIntBuilder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.LongType") // bigint
        arr->push_back(std::make_shared<arrow::Int64Builder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.ShortType") // smallint
        arr->push_back(std::make_shared<arrow::Int16Builder>(pool));
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.SimpleDateType") // date
    //     arr->push_back(std::make_shared<arrow::TimestampBuilder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.TimeType") // time
        arr->push_back(std::make_shared<arrow::Int64Builder>(pool));
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.TimeUUIDType") // timeuuid
    //     arr->push_back(std::make_shared<arrow::FixedSizeBinaryBuilder>(pool));
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.TimestampType") // timestamp
    // arr->push_back(std::make_shared<arrow::TimestampBuilder>(pool));
    else if (cassandra_type == "org.apache.cassandra.db.marshal.UTF8Type") // text, varchar
        arr->push_back(std::make_shared<arrow::StringBuilder>(pool));
    // else if (cassandra_type == "org.apache.cassandra.db.marshal.UUIDType") // uuid
    //     arr->push_back(std::make_shared<arrow::StructBuilder>(pool));

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
    // else if (cassandra_type.rfind(maptype, 0) == 0) // map<type, type>
    // {
    //     // TODO this currently only works if both the key and value are simple types, i.e. not maps
    //     const int sep_idx = cassandra_type.find(',');
    //     std::string key_type = std::string(cassandra_type.begin() + maptype.size() + 1, cassandra_type.begin() + sep_idx);
    //     std::string value_type = std::string(cassandra_type.begin() + sep_idx + 1, cassandra_type.end() - 1);
    //     std::cout << key_type << ": " << value_type << '\n';
    // }
    // else if (cassandra_type.rfind(settype, 0) == 0) // set<type>
    //     arr->push_back(std::make_shared<arrow::UInt32Builder>(pool));
    // else if (cassandra_type.rfind(listtype, 0) == 0) // list<type>
    //     arr->push_back(std::make_shared<arrow::UInt32Builder>(pool));

    else
    {
        perror("unrecognized type:\n");
        perror(cassandra_type.c_str());
        exit(1);
    }
}

arrow::Status append_to_builder(str_arr_t &types, builder_arr_t &arr, int i, const std::string bytes, arrow::MemoryPool *pool)
{
    std::string cql_type = (*types)[i];
    std::cout << "appending: " << i << ", " << cql_type << "\n";

#define APPEND_VIA_TYPE(c_type, size)                                       \
    do                                                                      \
    {                                                                       \
        auto builder = (arrow::stl::CBuilderType<c_type> *)(*arr)[i].get(); \
        c_type val;                                                         \
        memcpy(&val, bytes.c_str(), size);                                  \
        ARROW_RETURN_NOT_OK(builder->Append(val));                          \
    } while (false)

    if (cql_type == "org.apache.cassandra.db.marshal.AsciiType" || cql_type == "org.apache.cassandra.db.marshal.BytesType" || cql_type == "org.apache.cassandra.db.marshal.UTF8Type") // ascii or blob or varchar or text
    {
        auto builder = (arrow::StringBuilder *)(*arr)[i].get();
        ARROW_RETURN_NOT_OK(builder->Append(bytes));
    }
    else if (cql_type == "org.apache.cassandra.db.marshal.BooleanType") // boolean
        APPEND_VIA_TYPE(bool, 1);
    else if (cql_type == "org.apache.cassandra.db.marshal.ByteType") // tinyint
        APPEND_VIA_TYPE(int8_t, 1);
    // else if (cql_type == "org.apache.cassandra.db.marshal.DecimalType") // decimal
    // {
    //     auto builder = (arrow::Decimal128Builder *)(*arr)[i].get();

    // }
    else if (cql_type == "org.apache.cassandra.db.marshal.DoubleType") // double
        APPEND_VIA_TYPE(double, 8);
    // else if (cql_type == "org.apache.cassandra.db.marshal.DurationType") // duration
    // {
    // }
    else if (cql_type == "org.apache.cassandra.db.marshal.FloatType") // float
        APPEND_VIA_TYPE(float, 4);
    // else if (cql_type == "org.apache.cassandra.db.marshal.InetAddressType") // inet
    // {
    // }
    else if (cql_type == "org.apache.cassandra.db.marshal.Int32Type") // int
        APPEND_VIA_TYPE(int32_t, 4);
    // else if (cql_type == "org.apache.cassandra.db.marshal.IntegerType") // varint
    // {
    // }
    else if (cql_type == "org.apache.cassandra.db.marshal.LongType") // bigint
    {
        auto builder = (arrow::Int64Builder *)(*arr)[i].get();
        long long val;
        memcpy(&val, bytes.c_str(), 8);
        ARROW_RETURN_NOT_OK(builder->Append(val));
    }
    else if (cql_type == "org.apache.cassandra.db.marshal.ShortType") // smallint
        APPEND_VIA_TYPE(short, 2);
    // else if (cql_type == "org.apache.cassandra.db.marshal.SimpleDateType") // date
    // {
    // }
    // else if (cql_type == "org.apache.cassandra.db.marshal.TimeType") // time
    // {
    // }
    // else if (cql_type == "org.apache.cassandra.db.marshal.TimeUUIDType") // timeuuid
    // {
    // }
    // else if (cql_type == "org.apache.cassandra.db.marshal.TimestampType") // timestamp
    // {
    // }
    // else if (cql_type == "org.apache.cassandra.db.marshal.UUIDType") // uuid
    // {
    // }

    else
    {
        perror("unrecognized type:\n");
        perror(cql_type.c_str());
        exit(1);
    }

#undef APPEND_VIA_TYPE

    return arrow::Status::OK();
}

std::map<std::string, std::shared_ptr<arrow::DataType>> cql_to_arrow_type{

    {"org.apache.cassandra.db.marshal.AsciiType", arrow::utf8()},
    {"org.apache.cassandra.db.marshal.BooleanType", arrow::boolean()},
    {"org.apache.cassandra.db.marshal.ByteType", arrow::int8()},
    {"org.apache.cassandra.db.marshal.BytesType", arrow::binary()},
    // {"org.apache.cassandra.db.marshal.CompositeType", arrow:: },
    // {"org.apache.cassandra.db.marshal.CounterColumnType", arrow:: },
    // {"org.apache.cassandra.db.marshal.DateType", arrow:: },
    // {"org.apache.cassandra.db.marshal.DecimalType", arrow:: },
    {"org.apache.cassandra.db.marshal.DoubleType", arrow::float64()},
    // {"org.apache.cassandra.db.marshal.DurationType", arrow:: },
    // {"org.apache.cassandra.db.marshal.DynamicCompositeType", arrow:: },
    // {"org.apache.cassandra.db.marshal.EmptyType", arrow:: },
    {"org.apache.cassandra.db.marshal.FloatType", arrow::float32()},
    // {"org.apache.cassandra.db.marshal.FrozenType", arrow:: },
    // {"org.apache.cassandra.db.marshal.InetAddressType", arrow:: },
    {"org.apache.cassandra.db.marshal.Int32Type", arrow::int32()},
    // {"org.apache.cassandra.db.marshal.IntegerType", arrow:: },
    // {"org.apache.cassandra.db.marshal.LexicalUUIDType", arrow:: },
    // {TODO ListType, arrow:: },
    {"org.apache.cassandra.db.marshal.LongType", arrow::int64()},
    // {TODO MapType, arrow:: },
    // {"org.apache.cassandra.db.marshal.PartitionerDefinedOrder", arrow:: },
    // {"org.apache.cassandra.db.marshal.ReversedType", arrow:: },
    // {TODO SetType, arrow:: },
    {"org.apache.cassandra.db.marshal.ShortType", arrow::int16()},
    // {"org.apache.cassandra.db.marshal.SimpleDateType", arrow:: },
    // {"org.apache.cassandra.db.marshal.TimeType", arrow:: },
    // {"org.apache.cassandra.db.marshal.TimeUUIDType", arrow:: },
    // {"org.apache.cassandra.db.marshal.TimestampType", arrow:: },
    // {"org.apache.cassandra.db.marshal.TupleType", arrow:: },
    {"org.apache.cassandra.db.marshal.UTF8Type", arrow::utf8()},
    // {"org.apache.cassandra.db.marshal.UUIDType", arrow:: },
    // {"org.apache.cassandra.db.marshal.UserType", arrow:: },
};

arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Schema> *schema, std::shared_ptr<arrow::Table> *table)
{
    arrow::MemoryPool *pool = arrow::default_memory_pool();

    auto &ptr = (*statistics->toc()->array())[3];
    auto body = (sstable_statistics_t::serialization_header_t *)ptr->body();

    str_arr_t types = std::make_shared<std::vector<std::string>>();
    str_arr_t names = std::make_shared<std::vector<std::string>>();
    builder_arr_t arr = std::make_shared<std::vector<std::shared_ptr<arrow::ArrayBuilder>>>();

    std::cout << "saving partition key\n";
    create_builder(types, names, arr, body->partition_key_type()->body(), "partition key", pool);

    std::cout << "saving clustering keys\n";
    for (auto &col : *body->clustering_key_types()->array())
        create_builder(types, names, arr, col->body(), "clustering key", pool);

    // TODO handle static columns
    std::cout << "saving regular columns\n";
    for (auto &col : *body->regular_columns()->array())
        create_builder(types, names, arr, col->column_type()->body(), col->name()->body(), pool);

    for (std::unique_ptr<sstable_data_t::partition_t> &partition : *sstable->partitions())
    {
        for (std::unique_ptr<sstable_data_t::unfiltered_t> &unfiltered : *partition->unfiltereds())
        {
            if ((unfiltered->flags() & 0x01) != 0)
                break;
            if ((unfiltered->flags() & 0x02) != 0)
                continue;

            sstable_data_t::row_t *row = (sstable_data_t::row_t *)unfiltered->body();

            int idx = 0;
            int kind = deserialization_helper_t::REGULAR;
            if (((unfiltered->flags() & 0x80) != 0) && ((row->extended_flags() & 0x01) != 0))
            {
                kind = deserialization_helper_t::STATIC;
            }

            ARROW_RETURN_NOT_OK(append_to_builder(types, arr, idx++, partition->header()->key(), pool));
            for (auto &cell : *row->clustering_blocks()->values())
            {
                ARROW_RETURN_NOT_OK(append_to_builder(types, arr, idx++, cell, pool));
            }

            for (auto &cell : *row->cells())
            {
                auto simple_cell = (sstable_data_t::simple_cell_t *)cell.get();
                ARROW_RETURN_NOT_OK(append_to_builder(types, arr, idx++, simple_cell->value()->value(), pool));
            }
        }
    }

    int n = arr->size();
    std::cout << n << '\n';
    std::vector<std::shared_ptr<arrow::Array>> finished_arrays;
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;

    for (int i = 0; i < n; ++i)
    {
        std::shared_ptr<arrow::Array> arrptr;
        ARROW_RETURN_NOT_OK((*arr)[i]->Finish(&arrptr));
        finished_arrays.push_back(arrptr);
        auto type_ptr = cql_to_arrow_type.find((*types)[i]);
        if (type_ptr == cql_to_arrow_type.end())
        {
            std::cerr << (*types)[i];
            perror("type not found");
            exit(1);
        }
        schema_vector.push_back(arrow::field((*names)[i], type_ptr->second));
    }

    *schema = std::make_shared<arrow::Schema>(schema_vector);

    *table = arrow::Table::Make(*schema, finished_arrays);

    return arrow::Status::OK();
}
