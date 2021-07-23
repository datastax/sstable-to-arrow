#include "io.h"

const int PORT = 9143;

/**
 * @brief Send an Arrow Table across a network socket.
 * 
 * @param schema the schema (field types and names) of the table
 * @param table the Arrow Table containing the SSTable data
 * @return arrow::Status 
 */
arrow::Status send_tables(const std::vector<std::shared_ptr<arrow::Table>> &tables)
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
    int cli_sockfd;
    FAIL_ON_STATUS(cli_sockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen), "error on accept");

    char buffer[256];
    memset(buffer, 0x00, 256);
    std::cout << "waiting for message\n";
    FAIL_ON_STATUS(read(cli_sockfd, buffer, 255), "error reading from socket");

    // char *bytes = htobebytes(tables.size(), sizeof(size_t));
    size_t ntables = SIZE_TO_BE(tables.size());
    FAIL_ON_STATUS(write(cli_sockfd, (char *)&ntables, sizeof(size_t)), "failed writing number of tables");
    for (auto table : tables)
        send_table(table, cli_sockfd);

    DEBUG_ONLY(std::cout << "closing sockets\n");

    close(cli_sockfd);
    close(sockfd);

    DEBUG_ONLY(std::cout << "closed sockets\n");

    return arrow::Status::OK();
}

arrow::Status send_table(std::shared_ptr<arrow::Table> table, int cli_sockfd)
{
    ARROW_ASSIGN_OR_RAISE(auto ostream, arrow::io::BufferOutputStream::Create());

    DEBUG_ONLY(std::cout << "making stream writer\n");
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(ostream, table->schema()));

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
    ARROW_ASSIGN_OR_RAISE(auto bytes, ostream->Finish())
    DEBUG_ONLY(std::cout << "buffer size (number of bytes written): " << bytes->size() << '\n');

    // char *table_size = htobebytes(bytes->size(), sizeof(size_t));
    size_t table_size = SIZE_TO_BE(bytes->size());
    FAIL_ON_STATUS(write(cli_sockfd, (char *)&table_size, sizeof(size_t)), "error writing size to socket");
    FAIL_ON_STATUS(write(cli_sockfd, (char *)bytes->data(), bytes->size()), "error writing to socket");

    return arrow::Status::OK();
}

/**
 * @brief Write an arrow data to a parquet file.
 * 
 * @param table 
 * @param pool 
 * @return arrow::Status 
 */
arrow::Status write_parquet(std::shared_ptr<arrow::Table> table, arrow::MemoryPool *pool)
{
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open("table.parquet"));
    return parquet::arrow::WriteTable(*table, pool, outfile, 3);
}