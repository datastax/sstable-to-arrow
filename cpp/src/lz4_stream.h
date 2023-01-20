#ifndef LZ4_STREAM
#define LZ4_STREAM

// LZ4 Headers
//#include <lz4frame.h>
#include <lz4.h>                                       // for LZ4_decompres...

// Standard headers
#include <array>
#include <cassert>
#include <functional>
#include <iostream>
#include <memory>
#include <streambuf>
#include <vector>
#include "sstable_compression_info.h" // for sstable_compression_info_t


namespace lz4_stream {

/**
 * @brief An input stream that will LZ4 decompress output data.
 *
 * An input stream that will wrap another input stream and LZ4
 * decompress its output data to that stream.
 *
 */
template <size_t SrcBufSize = 256, size_t DestBufSize = 256>
class basic_istream : public std::istream
{
 public:
  /**
   * @brief Constructs an LZ4 decompression input stream
   *
   * @param source The stream to read LZ4 compressed data from
   */
  basic_istream(std::istream& source, std::unique_ptr<sstable_compression_info_t> compression_info)
    : std::istream(new input_buffer(source, compression_info)),
      buffer_(dynamic_cast<input_buffer*>(rdbuf())) {
    assert(buffer_);
  }

  /**
   * @brief Destroys the LZ4 output stream.
   */
  ~basic_istream() {
    delete buffer_;
  }

 private:
  class input_buffer : public std::streambuf {
  public:
    input_buffer(std::istream &source,std::unique_ptr<sstable_compression_info_t>& compression_info)
      : source_(source),
        compression_info_(std::move(compression_info)),
        offset_(0),
        src_buf_size_(0)
        {
      setg(&src_buf_.front(), &src_buf_.front(), &src_buf_.front());
    }

    ~input_buffer() {
    }

    int_type underflow() override {
      size_t nchunks = compression_info_->chunk_count();
      const auto &offsets = *compression_info_->chunk_offsets();
      size_t chunk_length = compression_info_->chunk_length();

      auto i = 0;
      uint64_t offset = offsets[i];

      int64_t src_size = source_.tellg(); // the total size of the uncompressed file
      // get size based on offset with special case for last chunk
      uint64_t chunk_size = (i == nchunks - 1 ? src_size : offsets[i + 1]) - offset;
      // TODO: fix for actual last chunk
      //uint64_t chunk_size = (offsets[i + 1]) - offset;

      std::vector<char> buffer(chunk_size);

      // skip 4 bytes written by Cassandra at beginning of each chunk and 4
      // bytes at end for Adler32 checksum
      source_.seekg(offset + 4, std::ios::beg);
      source_.read(buffer.data(), chunk_size - 8);

      int ntransferred =
          LZ4_decompress_safe(&src_buf_.front(), &dest_buf_.front(), chunk_size - 8, chunk_length);

      if (ntransferred < 0)
          throw std::runtime_error(std::string(
            "decompression of block " + std::to_string(i) +
            " failed with error code " + std::to_string(ntransferred)
          ));

      setg(&dest_buf_.front(), &dest_buf_.front(), &dest_buf_.front() + chunk_length);
      return traits_type::to_int_type(*gptr());
    }

    input_buffer(const input_buffer&) = delete;
    input_buffer& operator= (const input_buffer&) = delete;
  private:
    std::istream& source_;
    std::unique_ptr<sstable_compression_info_t> compression_info_;
    std::array<char, SrcBufSize> src_buf_;
    std::array<char, DestBufSize> dest_buf_;
    size_t offset_;
    size_t src_buf_size_;
  };

  input_buffer* buffer_;
};

using istream = basic_istream<>;

}
#endif // LZ4_STREAM