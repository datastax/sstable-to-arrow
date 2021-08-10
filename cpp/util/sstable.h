#ifndef SSTABLE_H_
#define SSTABLE_H_

#include <arrow/result.h>        // for ARROW_ASSIGN_OR_RAISE, Result
#include <arrow/status.h>        // for Status
#include <bits/exception.h>      // for exception
#include <kaitai/kaitaistream.h> // for kstream

#include <fstream> // for istream
#include <memory>  // for allocator, unique_ptr, make_un...
#include <string>  // for string, operator+, char_traits
#include <utility> // for move
#include <vector>  // for vector

#include "sstable_compression_info.h" // for sstable_compression_info_t
#include "sstable_data.h"             // for sstable_data_t
#include "sstable_index.h"            // for sstable_index_t
#include "sstable_statistics.h"       // for sstable_statistics_t
#include "sstable_summary.h"          // for sstable_summary_t

/**
 * @brief Opens an input stream to the file specified by `path` and stores it
 * in the object pointed to by `ifs`.
 *
 * @param path the file to open an input stream to
 * @param ifs where to store the resulting input stream
 */
arrow::Result<std::unique_ptr<std::istream>> open_stream(const std::string &path);

template <typename T> class file_container
{
    std::string m_path;
    std::unique_ptr<T> m_file;
    std::unique_ptr<std::istream> m_stream;
    std::unique_ptr<kaitai::kstream> m_ks;

  public:
    // creates the kaitai object from the stream specified by m_path
    arrow::Status init()
    {
        ARROW_ASSIGN_OR_RAISE(auto ptr, open_stream(m_path));
        return init(std::move(ptr));
    }

    // creates the kaitai object from the given stream and the type of this class
    arrow::Status init(std::unique_ptr<std::istream> stream)
    {
        try
        {
            m_stream = std::move(stream);
            m_ks = std::make_unique<kaitai::kstream>(m_stream.get());
            m_file = std::make_unique<T>(m_ks.get());
            return arrow::Status::OK();
        }
        catch (const std::exception &err)
        {
            return arrow::Status::SerializationError("error reading sstable file \"" + m_path + "\": " + err.what());
        }
    }

    const std::string &path() const
    {
        return m_path;
    }

    const std::unique_ptr<T> &file() const
    {
        return m_file;
    }

    void set_path(const std::string &path)
    {
        m_path = path;
    }
};

class sstable_t
{
    std::vector<char> m_decompressed_data;
    file_container<sstable_statistics_t> m_statistics;
    file_container<sstable_data_t> m_data;
    file_container<sstable_index_t> m_index;
    file_container<sstable_summary_t> m_summary;
    file_container<sstable_compression_info_t> m_compression_info;

  public:
    // reads the kaitai objects from the file paths or streams
    // stored in the member objects
    arrow::Status init();

    // initializes the data object
    // requires that the compression_info is already loaded
    arrow::Status read_decompressed_sstable();

    // the following members expose the actual kaitai objects contained within
    // the file container
    const std::unique_ptr<sstable_statistics_t> &statistics() const;
    const std::unique_ptr<sstable_data_t> &data() const;
    const std::unique_ptr<sstable_index_t> &index() const;
    const std::unique_ptr<sstable_summary_t> &summary() const;
    const std::unique_ptr<sstable_compression_info_t> &compression_info() const;

    void set_statistics_path(const std::string &path);
    void set_data_path(const std::string &path);
    void set_index_path(const std::string &path);
    void set_summary_path(const std::string &path);
    void set_compression_info_path(const std::string &path);
};

void init_deserialization_helper(sstable_statistics_t::serialization_header_t *serialization_header);

#endif
