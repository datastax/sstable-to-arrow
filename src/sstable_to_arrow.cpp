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

    // auto maybe_file = arrow::io::FileOutputStream::Open("data.bin");
    // if (!maybe_file.ok())
    // {
    //     perror("couldn't open filesystem");
    //     exit(1);
    // }
    // auto file = *maybe_file;

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

void create_builder(str_arr_t &types, str_arr_t &names, builder_arr_t &arr, const std::string cqltype, const std::string name, arrow::MemoryPool *pool)
{
    std::cout << cqltype << ", " << name << "\n";
    types->push_back(cqltype);
    names->push_back(name);
    if (cqltype == "org.apache.cassandra.db.marshal.FloatType")
    {
        arr->push_back(std::make_shared<arrow::FloatBuilder>(pool));
    }
    else if (cqltype == "org.apache.cassandra.db.marshal.Int32Type")
    {
        arr->push_back(std::make_shared<arrow::UInt32Builder>(pool));
    }
    else if (cqltype == "org.apache.cassandra.db.marshal.AsciiType")
    {
        arr->push_back(std::make_shared<arrow::StringBuilder>(pool));
    }
    else
    {
        perror("unrecognized type:\n");
        perror(cqltype.c_str());
        exit(1);
    }
}

arrow::Status append_to_builder(str_arr_t &types, builder_arr_t &arr, int i, const std::string bytes, arrow::MemoryPool *pool)
{
    std::cout << "appending: " << i << ", " << (*types)[i] << "\n";
    if ((*types)[i] == "org.apache.cassandra.db.marshal.FloatType")
    {
        auto builder = (arrow::FloatBuilder *)(*arr)[i].get();
        kaitai::kstream ks(bytes);
        basic_types_t::f4_t val(&ks);
        ARROW_RETURN_NOT_OK(builder->Append(val.f4()));
    }
    else if ((*types)[i] == "org.apache.cassandra.db.marshal.Int32Type")
    {
        auto builder = (arrow::UInt32Builder *)(*arr)[i].get();
        kaitai::kstream ks(bytes);
        basic_types_t::u4_t val(&ks);
        ARROW_RETURN_NOT_OK(builder->Append(val.u4()));
    }
    else if ((*types)[i] == "org.apache.cassandra.db.marshal.AsciiType")
    {
        auto builder = (arrow::StringBuilder *)(*arr)[i].get();
        ARROW_RETURN_NOT_OK(builder->Append(bytes));
    }
    else
    {
        perror("unrecognized type:\n");
        perror((*types)[i].c_str());
        exit(1);
    }
    return arrow::Status::OK();
}

std::shared_ptr<arrow::DataType> get_arrow_type(const std::string t)
{
    if (t == "org.apache.cassandra.db.marshal.FloatType")
        return arrow::float32();
    else if (t == "org.apache.cassandra.db.marshal.Int32Type")
        return arrow::uint32();
    else if (t == "org.apache.cassandra.db.marshal.AsciiType")
        return arrow::utf8();
    perror("unrecognized type:");
    perror(t.c_str());
    exit(1);
    return nullptr;
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
        schema_vector.push_back(arrow::field((*names)[i], get_arrow_type((*types)[i])));
    }

    *schema = std::make_shared<arrow::Schema>(schema_vector);

    *table = arrow::Table::Make(*schema, finished_arrays);

    return arrow::Status::OK();
}
