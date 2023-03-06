#include "io.h"

#include <arrow/io/file.h>        // for FileOutputStream
#include <arrow/io/memory.h>      // for BufferOutputStream
#include <arrow/ipc/writer.h>     // for RecordBatchWriter, WriteStats, Mak...
#include <arrow/result.h>         // for ARROW_ASSIGN_OR_RAISE, Result
#include <arrow/table.h>          // for ConcatenateTables, ConcatenateTabl...
#include <netinet/in.h>           // for sockaddr_in, htons, INADDR_ANY
#include <parquet/arrow/writer.h> // for WriteTable
#include <parquet/exception.h>    // for PARQUET_ASSIGN_OR_THROW
#include <string.h>               // for memset
#include <sys/socket.h>           // for accept, bind, listen, setsockopt
#include <unistd.h>               // for write, close, read
#include <arrow/memory_pool.h>    // For MemoryPool

#include <iostream> // for operator<<, basic_ostream::operator<<

#include "opts.h" // for DEBUG_ONLY
namespace arrow
{
class MemoryPool;
} // namespace arrow

namespace sstable_to_arrow
{
namespace io
{

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
    FAIL_ON_STATUS(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option)),
                   "failed setting socket options");
    std::cout << "created socket at file descriptor " << sockfd << '\n';

    struct sockaddr_in serv_addr;
    memset((char *)&serv_addr, 0x00, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(PORT);

    FAIL_ON_STATUS(bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)), "error on binding");

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
        ARROW_RETURN_NOT_OK(send_table(table, cli_sockfd));

    DEBUG_ONLY("closing sockets\n");

    close(cli_sockfd);
    close(sockfd);

    DEBUG_ONLY("closed sockets\n");

    return arrow::Status::OK();
}

arrow::Status send_table(std::shared_ptr<arrow::Table> table, int cli_sockfd)
{
    ARROW_ASSIGN_OR_RAISE(auto ostream, arrow::io::BufferOutputStream::Create());

    DEBUG_ONLY("making stream writer\n");
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(ostream, table->schema()));

    ARROW_RETURN_NOT_OK(writer->WriteTable(*table, -1));
    DEBUG_ONLY("writer stats:\n\tnum dictionary batches: " + std::to_string(writer->stats().num_dictionary_batches) +
               "\n\tnum dictionary deltas: " + std::to_string(writer->stats().num_dictionary_deltas) +
               "\n\tnum messages: " + std::to_string(writer->stats().num_messages) +
               "\n\tnum record batches: " + std::to_string(writer->stats().num_record_batches) +
               "\n\tnum replaced dictionaries: " + std::to_string(writer->stats().num_replaced_dictionaries) + '\n');
    ARROW_RETURN_NOT_OK(writer->Close());

    DEBUG_ONLY("finishing stream\n");
    ARROW_ASSIGN_OR_RAISE(auto bytes, ostream->Finish())
    DEBUG_ONLY("buffer size (number of bytes written): " + std::to_string(bytes->size()) + '\n');

    // char *table_size = htobebytes(bytes->size(), sizeof(size_t));
    size_t table_size = SIZE_TO_BE(bytes->size());
    FAIL_ON_STATUS(write(cli_sockfd, (char *)&table_size, sizeof(size_t)), "error writing size to socket");
    FAIL_ON_STATUS(write(cli_sockfd, (char *)bytes->data(), bytes->size()), "error writing to socket");

    return arrow::Status::OK();
}


arrow::Status write_parquet(const std::string &path, std::shared_ptr<arrow::RecordBatchReader> reader,
                            arrow::MemoryPool *pool)
{
    using parquet::ArrowWriterProperties;
    using parquet::WriterProperties;

    // Choose compression
    std::shared_ptr<WriterProperties> props =
        WriterProperties::Builder().compression(arrow::Compression::UNCOMPRESSED)->build();
        //WriterProperties::Builder().compression(arrow::Compression::LZ4)->build();

    // Opt to store Arrow schema for easier reads back into Arrow
    std::shared_ptr<ArrowWriterProperties> arrow_props = ArrowWriterProperties::Builder().store_schema()->build();

    // Create a writer
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(path));
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    ARROW_RETURN_NOT_OK(
        parquet::arrow::FileWriter::Open(*reader->schema().get(), pool, outfile, props, arrow_props, &writer));

    // Write each batch as a row_group
    int batch_num = 0;
    for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *reader)
    {
        std::cout << "BATCH NUMBER " << batch_num++ << "\n";
        ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
        ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatches(batch->schema(), {batch}));
        ARROW_RETURN_NOT_OK(writer->WriteTable(*table.get(), batch->num_rows()));
    }

    ARROW_RETURN_NOT_OK(writer->Close());
    ARROW_RETURN_NOT_OK(outfile->Close());

    return arrow::Status::OK();
}

/**
 * @brief Write arrow data to a parquet file.
 *
 * @param table the arrow table to write
 */

arrow::Status write_parquet(const std::string &path, std::vector<std::shared_ptr<arrow::Table>> tables,
                            arrow::MemoryPool *pool)
{
    // How much memory used used at this point
    std::cout << "Memory prior to concatenation: " << pool->bytes_allocated() << " bytes" << std::endl;

    // What are the schemas?
    for (int i = 0; i < tables.size(); ++i) {
        std::cout << "Table " << i << " schema:\n" << tables[i]->schema()->ToString() << std::endl;
    }

    arrow::ConcatenateTablesOptions options;
    options.unify_schemas = true;
    ARROW_ASSIGN_OR_RAISE(auto final_table, arrow::ConcatenateTables(tables, options));

    // How much memory used used at this point?
    std::cout << "Memory after concatenation: " << pool->bytes_allocated() << " bytes" << std::endl;

    // What is the unified schema?
    std::cout << "Combined table schema:\n" << final_table->schema()->ToString() << std::endl;

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(path));
    return parquet::arrow::WriteTable(*final_table, pool, outfile, 1000000);
}

} // namespace io
} // namespace sstable_to_arrow
