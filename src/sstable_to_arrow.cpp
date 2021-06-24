// See http://arrow.apache.org/docs/cpp/examples/row_columnar_conversion.html

#include "sstable_to_arrow.h"
#include <iostream>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <arrow/filesystem/api.h>
#include <sstream>
#include "clustering_blocks.h"
#define FAIL_ON_STATUS(x, msg) \
    if ((x) < 0)               \
    {                          \
        perror((msg));         \
        exit(1);               \
    }

using arrow::StringBuilder;
using std::unique_ptr;

const int PORT = 65432;

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

arrow::Status vector_to_columnar_table(const sstable_data_t *sstable, std::shared_ptr<arrow::Schema> *schema, std::shared_ptr<arrow::Table> *table)
{
    arrow::MemoryPool *pool = arrow::default_memory_pool();

    StringBuilder key_builder(pool);
    StringBuilder teacher_builder(pool);
    StringBuilder level_builder(pool);

    for (unique_ptr<sstable_data_t::partition_t> &partition : *sstable->partitions())
    {
        for (unique_ptr<sstable_data_t::unfiltered_t> &unfiltered : *partition->unfiltereds())
        {
            if ((unfiltered->flags() & 0x01) != 0)
                break;
            if ((unfiltered->flags() & 0x02) != 0)
                continue;

            sstable_data_t::row_t *row = (sstable_data_t::row_t *)unfiltered->body();

            int kind = deserialization_helper_t::REGULAR;
            if (((unfiltered->flags() & 0x80) != 0) && ((row->extended_flags() & 0x01) != 0))
            {
                kind = deserialization_helper_t::STATIC;
            }

            ARROW_RETURN_NOT_OK(key_builder.Append(partition->header()->key()));
            for (std::string &cell : *row->clustering_blocks()->values())
            {
                ARROW_RETURN_NOT_OK(teacher_builder.Append(cell));
            }

            int i = 0;
            for (auto &cell : *row->cells())
            {
                std::string col_type = deserialization_helper_t::get_col_type(kind, i++);
                auto simple_cell = (sstable_data_t::simple_cell_t *)cell.get();
                ARROW_RETURN_NOT_OK(level_builder.Append(simple_cell->value()->value()));
            }
        }
    }

    std::shared_ptr<arrow::Array> key_array;
    ARROW_RETURN_NOT_OK(key_builder.Finish(&key_array));
    std::shared_ptr<arrow::Array> teacher_array;
    ARROW_RETURN_NOT_OK(teacher_builder.Finish(&teacher_array));
    std::shared_ptr<arrow::Array> level_array;
    ARROW_RETURN_NOT_OK(level_builder.Finish(&level_array));

    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {arrow::field("key", arrow::utf8()),
                                                                arrow::field("teacher", arrow::utf8()),
                                                                arrow::field("level", arrow::utf8())};

    *schema = std::make_shared<arrow::Schema>(schema_vector);

    *table = arrow::Table::Make(*schema, {key_array, teacher_array, level_array});

    return arrow::Status::OK();
}
