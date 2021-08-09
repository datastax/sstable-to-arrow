#include "main.h"

#define EXIT_NOT_OK(expr, msg)                                  \
    do                                                          \
    {                                                           \
        arrow::Status _s = (expr);                              \
        if (!_s.ok())                                           \
        {                                                       \
            std::cerr << (msg) << ": " << _s.message() << '\n'; \
            return 1;                                           \
        }                                                       \
    } while (0)

const int MAX_FILEPATH_SIZE = 45;

int main(int argc, char *argv[])
{
    read_options(argc, argv);

    if (global_flags.errors.size() > 0)
    {
        std::cerr << "invalid arguments:\n";
        for (const std::string &err : global_flags.errors)
            std::cerr << err << '\n';
        return 1;
    }

    EXIT_NOT_OK(run_arguments(), "error running arguments");

    if (!global_flags.read_sstable_dir)
        return 0;

    s3_connection conn;
    if (!conn.ok())
    {
        std::cerr << "error connecting to S3\n";
        return 1;
    }

    std::map<int, std::shared_ptr<sstable_t>> sstables;
    if (global_flags.is_s3)
        EXIT_NOT_OK(get_file_paths_from_s3(global_flags.sstable_dir_path.string(), sstables), "error loading from S3");
    else
        get_file_paths(global_flags.sstable_dir_path, sstables);

    if (sstables.empty())
    {
        std::cerr << "no sstables found\n";
        return 1;
    }

    std::vector<std::shared_ptr<arrow::Table>> finished_tables(sstables.size());

    int i = 0;
    for (auto &entry : sstables)
    {
        std::cout << "\n\n========== Reading SSTable #" << entry.first << " ==========\n";
        EXIT_NOT_OK(load_sstable_files(entry.second), "error loading sstable files");

        auto start_ts = std::chrono::high_resolution_clock::now();
        auto start = std::chrono::time_point_cast<std::chrono::microseconds>(start_ts).time_since_epoch().count();

        auto result = vector_to_columnar_table(entry.second->statistics, entry.second->data);

        auto end_ts = std::chrono::high_resolution_clock::now();
        auto end = std::chrono::time_point_cast<std::chrono::microseconds>(end_ts).time_since_epoch().count();
        std::cout << "[PROFILE conversion]: " << (end - start) << "us\n";

        EXIT_NOT_OK(result.status(), "error converting sstable");
        finished_tables[i++] = result.ValueOrDie();
    }

    if (global_flags.write_parquet)
    {
        EXIT_NOT_OK(write_parquet(global_flags.parquet_dst_path.string(), finished_tables), "error writing to parquet");
    }

    if (global_flags.listen)
    {
        EXIT_NOT_OK(send_tables(finished_tables), "error running I/O operations");
    }

    return 0;
}

arrow::Status run_arguments()
{
    if (global_flags.show_help)
    {
        std::cout << help_msg << '\n';
    }
    else if (global_flags.statistics_only)
    {
        ARROW_ASSIGN_OR_RAISE(auto statistics, read_sstable_file<sstable_statistics_t>(global_flags.statistics_path));
        debug_statistics(statistics);
    }
    else if (global_flags.index_only)
    {
        ARROW_ASSIGN_OR_RAISE(auto index, read_sstable_file<sstable_index_t>(global_flags.index_path));
        debug_index(index);
    }
    else if (global_flags.summary_only)
    {
        ARROW_ASSIGN_OR_RAISE(auto summary, read_sstable_file<sstable_summary_t>(global_flags.summary_path));
        debug_summary(summary);
    }
    return arrow::Status::OK();
}

s3_connection::s3_connection()
{
    std::cout << "opening connection to s3\n";
    m_ok = arrow::fs::EnsureS3Initialized().ok();
}

s3_connection::~s3_connection()
{
    std::cout << "closing connection to s3\n";
    m_ok = arrow::fs::FinalizeS3().ok();
}

bool s3_connection::ok() const
{
    return m_ok;
}

arrow::Status load_sstable_files(std::shared_ptr<sstable_t> sstable)
{
    // auto data_thread = std::async(read_data, sstable);
    // auto index_thread = std::async([&sstable]()
    //                                { return read_sstable_file<sstable_index_t>(sstable->index_path); });
    // auto summary_thread = std::async([&sstable]()
    //                                  { return read_sstable_file<sstable_summary_t>(sstable->summary_path); });

    ARROW_RETURN_NOT_OK(read_data(sstable));
    ARROW_ASSIGN_OR_RAISE(sstable->index, read_sstable_file<sstable_index_t>(sstable->index_path));
    ARROW_ASSIGN_OR_RAISE(sstable->summary, read_sstable_file<sstable_summary_t>(sstable->summary_path));

    if (sstable->statistics == nullptr || sstable->data == nullptr || sstable->index == nullptr || sstable->summary == nullptr)
        return arrow::Status::Invalid("Failed parsing SSTable");

    return arrow::Status::OK();
}

// ==================== READ SSTABLE FILES ====================
// These functions use kaitai to parse sstable files into C++
// objects.

arrow::Status read_data(std::shared_ptr<sstable_t> sstable)
{
    ARROW_ASSIGN_OR_RAISE(sstable->statistics, read_sstable_file<sstable_statistics_t>(sstable->statistics_path));
    init_deserialization_helper(get_serialization_header(sstable->statistics));

    auto start_ts = std::chrono::high_resolution_clock::now();
    auto start = std::chrono::time_point_cast<std::chrono::microseconds>(start_ts).time_since_epoch().count();

    if (sstable->compression_info_path != "") // if the SSTable is compressed
    {
        ARROW_ASSIGN_OR_RAISE(sstable->compression_info, read_sstable_file<sstable_compression_info_t>(sstable->compression_info_path));
        ARROW_ASSIGN_OR_RAISE(sstable->data, read_decompressed_sstable(sstable->compression_info, sstable->data_path));
    }
    else
    {
        ARROW_ASSIGN_OR_RAISE(sstable->data, read_sstable_file<sstable_data_t>(sstable->data_path));
    }

    auto end_ts = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::time_point_cast<std::chrono::microseconds>(end_ts).time_since_epoch().count();
    std::cout << "[PROFILE read_data]: " << (end - start) << "us\n";

    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<sstable_data_t>> read_decompressed_sstable(const std::shared_ptr<sstable_compression_info_t> &compression_info, const boost::filesystem::path &data_path)
{
    ARROW_ASSIGN_OR_RAISE(auto istream, open_stream(data_path));

    // get the number of compressed bytes
    istream->seekg(0, std::ios::end);
    int64_t src_size = static_cast<size_t>(istream->tellg()); // the total size of the uncompressed file
    std::cout << "compressed size: " << src_size << "\n";
    if (src_size < 0)
        throw std::runtime_error("error reading compressed data file");
    size_t nchunks = compression_info->chunk_count();

    std::vector<char> decompressed;

    const auto &offsets = *compression_info->chunk_offsets();
    size_t chunk_length = compression_info->chunk_length();

    size_t total_decompressed_size = 0;
    // loop through chunks to get total decompressed size (written by Cassandra)
    for (size_t i = 0; i < nchunks; ++i)
    {
        istream->seekg(offsets[i]);
        int32_t decompressed_size =
            ((istream->get() & 0xff) << 0x00) |
            ((istream->get() & 0xff) << 0x08) |
            ((istream->get() & 0xff) << 0x10) |
            ((istream->get() & 0xff) << 0x18);
        total_decompressed_size += decompressed_size;
    }
    std::cout << "total decompressed size: " << total_decompressed_size << '\n';
    decompressed.resize(total_decompressed_size);

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

        int ntransferred = LZ4_decompress_safe(
            buffer.data(), &decompressed[i * chunk_length],
            chunk_size - 8, chunk_length);

        if (ntransferred < 0)
            return arrow::Status::SerializationError("decompression of block " + std::to_string(i) + " failed with error code " + std::to_string(ntransferred));
    }

    // TODO see if can do it in a different way than putting the whole thing in a string
    std::string decompressed_data(decompressed.data(), decompressed.size());
    kaitai::kstream ks(decompressed_data);
    return std::make_shared<sstable_data_t>(&ks);
}

template <typename T>
arrow::Result<std::shared_ptr<T>> read_sstable_file(const boost::filesystem::path &path)
{
    try
    {
        ARROW_ASSIGN_OR_RAISE(auto istream, open_stream(path));
        kaitai::kstream ks(istream.get());
        return std::make_shared<T>(&ks);
    }
    catch (const std::exception &err)
    {
        return arrow::Status::SerializationError("error reading sstable file \"" + path.string() + "\": " + err.what());
    }
}

template <>
arrow::Result<std::shared_ptr<sstable_statistics_t>> read_sstable_file(const boost::filesystem::path &path)
{
    try
    {
        // TODO see if there is a better way to do this than static lifetime
        ARROW_ASSIGN_OR_RAISE(static auto istream, open_stream(path));
        istream->seekg(0, std::istream::end);
        std::cout << "num bytes: " << istream->tellg() << '\n';
        istream->seekg(0, std::istream::beg);
        static kaitai::kstream ks(istream.get());
        return std::make_shared<sstable_statistics_t>(&ks);
    }
    catch (const std::exception &err)
    {
        return arrow::Status::SerializationError("error reading sstable file \"" + path.string() + "\": " + err.what());
    }
}

/**
 * @brief Get the file paths of each SSTable file in a folder
 * Iterates through the specified folder and reads the file names into sstable
 * structs that map the file type (e.g. statistics, data) to the file
 * path.
 * @param path the path to the folder containing the SSTable files
 */
void get_file_paths(const boost::filesystem::path &dir_path, std::map<int, std::shared_ptr<sstable_t>> &sstables)
{
    namespace fs = boost::filesystem;
    for (const fs::directory_entry &file : fs::directory_iterator(dir_path))
        if (fs::is_regular_file(file.path()))
            add_file_to_sstables(file.path().string(), file.path().filename().string(), sstables);
}

arrow::Status get_file_paths_from_s3(const std::string &uri, std::map<int, std::shared_ptr<sstable_t>> &sstables)
{
    // get the bucket uri and the actual path to the file
    size_t pos = uri.find('/', 5);
    std::string bucket_uri{
        pos == std::string::npos
            ? uri
            : uri.substr(0, pos)};
    std::string path = uri.substr(5);

    ARROW_RETURN_NOT_OK(arrow::fs::EnsureS3Initialized());

    // create the S3 filesystem
    ARROW_ASSIGN_OR_RAISE(auto options, arrow::fs::S3Options::FromUri(bucket_uri));
    ARROW_ASSIGN_OR_RAISE(global_flags.s3fs, arrow::fs::S3FileSystem::Make(options));

    // get the list of files in the directory pointed to by `path`
    arrow::fs::FileSelector selector;
    selector.base_dir = path;
    ARROW_ASSIGN_OR_RAISE(auto file_info, global_flags.s3fs->GetFileInfo(selector));

    for (auto &info : file_info)
        if (info.IsFile())
            add_file_to_sstables(info.path(), info.base_name(), sstables);

    return arrow::Status::OK();
}

void add_file_to_sstables(const std::string &full_path, const std::string &file_name, std::map<int, std::shared_ptr<sstable_t>> &sstables)
{
    char fmt[5];
    int num;
    char db_type_buf[20]; // longest sstable file type specifier is "CompressionInfo"

    if (!boost::iends_with(file_name, ".db") || (file_name.size() > MAX_FILEPATH_SIZE))
    {
        std::cout << "skipping unrecognized file " << file_name << '\n';
        return;
    }

    if (sscanf(file_name.data(), "%2c-%d-big-%19[^.]", fmt, &num, db_type_buf) != 3) // number of arguments filled
    {
        std::cout << "Error reading formatted filename " << file_name << ", skipping\n";
        return;
    }

    std::string_view db_type(db_type_buf);

    if (sstables.count(num) == 0)
        sstables[num] = std::make_shared<sstable_t>();

    if (db_type == "Data")
        sstables[num]->data_path = full_path;
    else if (db_type == "Statistics")
        sstables[num]->statistics_path = full_path;
    else if (db_type == "Index")
        sstables[num]->index_path = full_path;
    else if (db_type == "Summary")
        sstables[num]->summary_path = full_path;
    else if (db_type == "CompressionInfo")
        sstables[num]->compression_info_path = full_path;
    else
        std::cout << "skipping unrecognized file " << file_name << '\n';
}

// ==================== HELPER FUNCTIONS ====================

arrow::Result<std::shared_ptr<std::istream>> open_stream(const boost::filesystem::path &path)
{
    if (global_flags.is_s3)
    {
        ARROW_ASSIGN_OR_RAISE(auto input_stream, global_flags.s3fs->OpenInputFile(path.string()));
        ARROW_ASSIGN_OR_RAISE(auto file_size, input_stream->GetSize());
        ARROW_ASSIGN_OR_RAISE(auto buffer, input_stream->Read(file_size));
        return std::make_shared<std::istringstream>(buffer->ToString(), std::istringstream::binary);
    }
    else
    {
        auto ss = std::make_shared<std::ifstream>(path.c_str(), std::ifstream::binary);
        if (!ss->is_open())
            return arrow::Status::IOError("could not open file \"" + path.string() + '\"');
        return ss;
    }
}

// Initialize the deserialization helper using the schema from the serialization
// header stored in the statistics file.
void init_deserialization_helper(sstable_statistics_t::serialization_header_t *serialization_header)
{
    // Set important constants for the serialization helper and initialize vectors to store
    // types of clustering columns
    auto clustering_key_types = serialization_header->clustering_key_types()->array();
    auto static_columns = serialization_header->static_columns()->array();
    auto regular_columns = serialization_header->regular_columns()->array();

    deserialization_helper_t::set_n_cols(deserialization_helper_t::CLUSTERING, clustering_key_types->size());
    deserialization_helper_t::set_n_cols(deserialization_helper_t::STATIC, static_columns->size());
    deserialization_helper_t::set_n_cols(deserialization_helper_t::REGULAR, regular_columns->size());

    int i;
    i = 0;
    for (auto &type : *clustering_key_types)
        deserialization_helper_t::set_col_type(deserialization_helper_t::CLUSTERING, i++, type->body());
    i = 0;
    for (auto &column : *static_columns)
        deserialization_helper_t::set_col_type(deserialization_helper_t::STATIC, i++, column->column_type()->body());
    i = 0;
    for (auto &column : *regular_columns)
        deserialization_helper_t::set_col_type(deserialization_helper_t::REGULAR, i++, column->column_type()->body());
}
