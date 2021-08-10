#include "sstable.h"

#include <arrow/buffer.h>          // for Buffer
#include <arrow/filesystem/s3fs.h> // for S3FileSystem
#include <arrow/io/interfaces.h>   // for RandomAccessFile
#include <assert.h>                // for assert
#include <lz4.h>                   // for LZ4_decompres...
#include <stddef.h>                // for size_t
#include <stdint.h>                // for uint64_t, int...

#include <boost/interprocess/streams/bufferstream.hpp> // for basic_ibuffer...
#include <iostream>                                    // for cout
#include <sstream>                                     // for basic_istring...

#include "conversion_helper.h"      // for get_serializa...
#include "deserialization_helper.h" // for deserializati...
#include "opts.h"                   // for flags, global...

arrow::Status sstable_t::init()
{
    m_statistics.init();
    init_deserialization_helper(get_serialization_header(statistics()));

    {
        timer t;
        if (m_compression_info.path() != "") // if the SSTable is compressed
        {
            m_compression_info.init();
            read_decompressed_sstable();
        }
        else
        {
            m_data.init();
        }
    }

    // TODO check for validity
    // if (sstable->statistics == nullptr || sstable->data == nullptr ||
    // sstable->index == nullptr || sstable->summary == nullptr) return
    // arrow::Status::Invalid("Failed parsing SSTable");

    return arrow::Status::OK();
}

arrow::Status sstable_t::read_decompressed_sstable()
{
    ARROW_ASSIGN_OR_RAISE(auto istream, open_stream(m_data.path()));

    // get the number of compressed bytes
    istream->seekg(0, std::ios::end);
    int64_t src_size = istream->tellg(); // the total size of the uncompressed file
    std::cout << "compressed size: " << src_size << "\n";
    assert(src_size >= 0);

    size_t nchunks = compression_info()->chunk_count();
    const auto &offsets = *compression_info()->chunk_offsets();
    size_t total_decompressed_size = 0;
    // loop through chunks to get total decompressed size (written by Cassandra)
    for (size_t i = 0; i < nchunks; ++i)
    {
        istream->seekg(offsets[i]);
        int32_t decompressed_size = ((istream->get() & 0xff) << 0x00) | ((istream->get() & 0xff) << 0x08) |
                                    ((istream->get() & 0xff) << 0x10) | ((istream->get() & 0xff) << 0x18);
        total_decompressed_size += decompressed_size;
    }
    std::cout << "total decompressed size: " << total_decompressed_size << '\n';
    m_decompressed_data.resize(total_decompressed_size);

    size_t chunk_length = compression_info()->chunk_length();
    for (size_t i = 0; i < nchunks; ++i) // read each chunk at a time
    {
        uint64_t offset = offsets[i];
        // get size based on offset with special case for last chunk
        uint64_t chunk_size = (i == nchunks - 1 ? src_size : offsets[i + 1]) - offset;

        std::vector<char> buffer(chunk_size);

        // skip 4 bytes written by Cassandra at beginning of each chunk and 4
        // bytes at end for Adler32 checksum
        istream->seekg(offset + 4, std::ios::beg);
        istream->read(buffer.data(), chunk_size - 8);

        int ntransferred =
            LZ4_decompress_safe(buffer.data(), &m_decompressed_data[i * chunk_length], chunk_size - 8, chunk_length);

        if (ntransferred < 0)
            return arrow::Status::SerializationError("decompression of block " + std::to_string(i) +
                                                     " failed with error code " + std::to_string(ntransferred));
    }

    // TODO see if can do it in a different way than putting the whole thing in a
    // string auto bs =
    // std::make_shared<boost::iostreams::stream<boost::iostreams::array_source>>(decompressed.data(),
    // decompressed.size()); naked new to convert from a boost stream to
    // std::istream
    auto bs = new boost::interprocess::ibufferstream(m_decompressed_data.data(), m_decompressed_data.size());
    auto is = std::unique_ptr<std::istream>(bs);
    if (!is)
    {
        return arrow::Status::IOError("Could not cast boost vector stream to std::istream");
    }

    m_data.init(std::move(is));

    return arrow::Status::OK();
}

const std::unique_ptr<sstable_statistics_t> &sstable_t::statistics() const
{
    return m_statistics.file();
}
const std::unique_ptr<sstable_data_t> &sstable_t::data() const
{
    return m_data.file();
}
const std::unique_ptr<sstable_index_t> &sstable_t::index() const
{
    return m_index.file();
}
const std::unique_ptr<sstable_summary_t> &sstable_t::summary() const
{
    return m_summary.file();
}
const std::unique_ptr<sstable_compression_info_t> &sstable_t::compression_info() const
{
    return m_compression_info.file();
}

void sstable_t::set_statistics_path(const std::string &path)
{
    m_statistics.set_path(path);
}
void sstable_t::set_data_path(const std::string &path)
{
    m_data.set_path(path);
}
void sstable_t::set_index_path(const std::string &path)
{
    m_index.set_path(path);
}
void sstable_t::set_summary_path(const std::string &path)
{
    m_summary.set_path(path);
}
void sstable_t::set_compression_info_path(const std::string &path)
{
    m_compression_info.set_path(path);
}

// Initialize the deserialization helper using the schema from the
// serialization header stored in the statistics file.
void init_deserialization_helper(sstable_statistics_t::serialization_header_t *serialization_header)
{
    // Set important constants for the serialization helper and initialize
    // vectors to store types of clustering columns
    auto *clustering_key_types = serialization_header->clustering_key_types()->array();
    auto *static_columns = serialization_header->static_columns()->array();
    auto *regular_columns = serialization_header->regular_columns()->array();

    deserialization_helper_t::set_n_cols(deserialization_helper_t::CLUSTERING, clustering_key_types->size());
    deserialization_helper_t::set_n_cols(deserialization_helper_t::STATIC, static_columns->size());
    deserialization_helper_t::set_n_cols(deserialization_helper_t::REGULAR, regular_columns->size());

    int i{0};
    for (auto &type : *clustering_key_types)
    {
        deserialization_helper_t::set_col_type(deserialization_helper_t::CLUSTERING, i++, type->body());
    }
    i = 0;
    for (auto &column : *static_columns)
    {
        deserialization_helper_t::set_col_type(deserialization_helper_t::STATIC, i++, column->column_type()->body());
    }
    i = 0;
    for (auto &column : *regular_columns)
    {
        deserialization_helper_t::set_col_type(deserialization_helper_t::REGULAR, i++, column->column_type()->body());
    }
}

arrow::Result<std::unique_ptr<std::istream>> open_stream(const std::string &path)
{
    if (global_flags.is_s3)
    {
        ARROW_ASSIGN_OR_RAISE(auto input_stream, global_flags.s3fs->OpenInputFile(path));
        ARROW_ASSIGN_OR_RAISE(auto file_size, input_stream->GetSize());
        ARROW_ASSIGN_OR_RAISE(auto buffer, input_stream->Read(file_size));
        return std::make_unique<std::istringstream>(buffer->ToString(), std::istringstream::binary);
    }
    else
    {
        auto *ifs = new std::ifstream(path.c_str(), std::ifstream::binary);
        if (!ifs->is_open())
        {
            return arrow::Status::IOError("could not open file \"" + path + '\"');
        }
        auto stream = std::unique_ptr<std::istream>(ifs);
        return stream;
    }
}
