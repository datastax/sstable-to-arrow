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
template <size_t SrcBufSize = 65536, size_t DestBufSize = 65536>
class basic_istream : public std::istream
{
 public:
  /**
   * @brief Constructs an LZ4 decompression input stream
   *
   * @param source The stream to read LZ4 compressed data from
   */
  basic_istream(std::unique_ptr<std::istream> source, std::unique_ptr<sstable_compression_info_t> compression_info)
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
    input_buffer(std::unique_ptr<std::istream>& source,std::unique_ptr<sstable_compression_info_t>& compression_info)
      : source_(std::move(source)),
        compression_info_(std::move(compression_info)),
        offset_(0),
        cur_offset_id_(0),
        src_buf_size_(0)
        {
      setg(&src_buf_.front(), &src_buf_.front(), &src_buf_.front());
      source_->seekg(0, std::ios::end);
      src_buf_size_ = source_->tellg();
      source_->seekg(0, std::ios::beg);

      nchunks = compression_info_->chunk_count();
      chunk_length = compression_info_->chunk_length();
    }

    ~input_buffer() {
    }

    int_type underflow() override {
      if (cur_offset_id_ >= nchunks)
      {
          return traits_type::eof();
      }
      const auto &offsets = *compression_info_->chunk_offsets();
      offset_ = offsets[cur_offset_id_];
      
      //std::cout << "offset: " << offset_ << "\n";
      //std::cout << "offset id: " << cur_offset_id_ << "\n";

      uint64_t chunk_size;
      if (cur_offset_id_ == nchunks - 1){
        // last chunk
        chunk_size = src_buf_size_ - offset_;
        // clear the source buffer to ensure we don't keep data from the last chunk
        std::fill(dest_buf_.begin(), dest_buf_.end(), 0);
      }
      else{
        chunk_size = offsets[cur_offset_id_ + 1] - offset_;
      }

      // skip 4 bytes written by Cassandra at beginning of each chunk and 4
      // bytes at end for Adler32 checksum
      source_->seekg(offset_ + 4, std::ios::beg);
      source_->read(src_buf_.data(), chunk_size - 8);

      //TODO: look into decompress_fast for performance
      int ntransferred =
          LZ4_decompress_safe(&src_buf_.front(), &dest_buf_.front(), chunk_size - 8, chunk_length);

      if (ntransferred < 0)
          throw std::runtime_error(std::string(
            "decompression of block " + std::to_string(cur_offset_id_) +
            " failed with error code " + std::to_string(ntransferred)
          ));

      if (cur_offset_id_ == nchunks - 1){
        size_t last_non_null = chunk_length;
        while (last_non_null > 0 && dest_buf_[last_non_null-1] == 0) {
            last_non_null--;
        }
        chunk_length = last_non_null;
      }
      setg(&dest_buf_.front(), &dest_buf_.front(), &dest_buf_.front() + chunk_length);
      cur_offset_id_++;
      return traits_type::to_int_type(*gptr());
    }

    std::streampos seekoff(std::streamoff off, std::ios_base::seekdir way, std::ios_base::openmode which) override
    {
      const auto &offsets = *compression_info_->chunk_offsets();
      int64_t src_size = source_->tellg();
      uint64_t chunk_size = (cur_offset_id_ == nchunks - 1 ? src_buf_size_ : offsets[cur_offset_id_ + 1]) - offset_;

      // Check which direction to move
      if (way == std::ios_base::beg)
      {
          seekpos(off, which);
      }
      else if (way == std::ios_base::cur)
      {
          //auto get_area_current = std::distance(dest_buf_.eback(), dest_buf_.gptr());
          auto get_area_current = std::distance(eback(), gptr());
          auto pos = off + get_area_current + cur_offset_id_ * chunk_length;
          seekpos(pos, which);
      }
      else
      {
          auto pos = src_size - off;
          seekpos(pos, which);
      }

      return pos_type(gptr() - eback());
    }

    std::streampos seekpos(std::streampos pos, std::ios_base::openmode which) override {
      const auto &offsets = *compression_info_->chunk_offsets();
      // calculate the cur_offset_id_ from the `pos`
      cur_offset_id_ = pos / chunk_length;
      if (cur_offset_id_ >= nchunks)
      {
          return traits_type::eof();
      }
      offset_ = offsets[cur_offset_id_];

      uint64_t chunk_size = (cur_offset_id_ == nchunks - 1 ? src_buf_size_ : offsets[cur_offset_id_ + 1]) - offset_;

      // skip 4 bytes written by Cassandra at beginning of each chunk and 4
      // bytes at end for Adler32 checksum
      source_->seekg(offset_ + 4, std::ios::beg);
      source_->read(src_buf_.data(), chunk_size - 8);

      int ntransferred =
          LZ4_decompress_safe(&src_buf_.front(), &dest_buf_.front(), chunk_size - 8, chunk_length);

      if (ntransferred < 0)
          throw std::runtime_error(std::string(
            "decompression of block " + std::to_string(cur_offset_id_) +
            " failed with error code " + std::to_string(ntransferred)
          ));


      setg(&dest_buf_.front(), &dest_buf_.front() + (pos % chunk_length),
           &dest_buf_.front() + chunk_length);

      cur_offset_id_++;

      // return the `pos`
      return pos;
    }

    input_buffer(const input_buffer&) = delete;
    input_buffer& operator= (const input_buffer&) = delete;
  private:
    std::unique_ptr<std::istream> source_;
    std::unique_ptr<sstable_compression_info_t> compression_info_;
    std::array<char, SrcBufSize> src_buf_;
    std::array<char, DestBufSize> dest_buf_;
    size_t offset_;
    size_t cur_offset_id_;
    uint64_t src_buf_size_;
    size_t nchunks;
    uint32_t chunk_length;

  };

  input_buffer* buffer_;
};

using istream = basic_istream<>;

}
#endif // LZ4_STREAM