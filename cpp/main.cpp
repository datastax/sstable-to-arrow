#include "main.h"

int main(int argc, char *argv[])
{
    read_options(argc, argv);
    if (global_flags.errors.size() > 0)
    {
        std::cerr << "invalid arguments:\n";
        for (std::string &err : global_flags.errors)
            std::cerr << err << '\n';
        return 1;
    }

    try
    {
        if (global_flags.index_only)
        {
            std::shared_ptr<sstable_index_t> index;
            read_sstable_file(global_flags.index_path, &index);
            if (index == nullptr)
                throw std::runtime_error("error reading index file");
            debug_index(index);
        }
        if (global_flags.statistics_only)
        {
            std::shared_ptr<sstable_statistics_t> statistics;
            read_sstable_file(global_flags.statistics_path, &statistics);
            if (statistics == nullptr)
                throw std::runtime_error("error reading statistics file");
            debug_statistics(statistics);
        }
        if (global_flags.summary_only)
        {
            std::shared_ptr<sstable_summary_t> summary;
            read_sstable_file(global_flags.summary_path, &summary);
            if (summary == nullptr)
                throw std::runtime_error("error reading summary file");
            debug_summary(summary);
        }
    }
    catch (const std::exception &err)
    {
        std::cerr << "error loading sstable file: " << err.what() << '\n';
        return 1;
    }

    if (!global_flags.read_sstable_dir)
        return 0;

    std::map<int, std::shared_ptr<sstable_t>> sstables;
    get_file_paths(global_flags.sstable_dir_path, sstables);

    std::vector<std::shared_ptr<arrow::Table>> finished_tables(sstables.size());

    int i = 0;
    for (auto it = sstables.begin(); it != sstables.end(); ++it)
    {
        std::cout << "\n\n========== Reading SSTable #" << it->first << " ==========\n";
        try
        {
            arrow::Status _s = process_sstable(it->second);
            if (!_s.ok())
                throw std::runtime_error(_s.ToString());
            arrow::Status status = vector_to_columnar_table(it->second->statistics, it->second->data, &finished_tables[i++]);
            if (!status.ok())
                throw std::runtime_error(status.ToString());
        }
        catch (const std::exception &e)
        {
            std::cerr << "error converting SSTable data to Arrow Table: " << e.what() << '\n';
            return 1;
        }
    }

    auto final_table_result = arrow::ConcatenateTables(finished_tables, arrow::ConcatenateTablesOptions{true});
    if (!final_table_result.ok())
    {
        std::cerr << "error concatenating sstables: " << final_table_result.status().ToString() << "\n";
        return 1;
    }
    auto final_table = final_table_result.ValueOrDie();

    if (global_flags.write_parquet)
    {
        try
        {
            arrow::Status _s = write_parquet(global_flags.parquet_dst_path, final_table);
            if (!_s.ok())
                throw std::runtime_error(_s.ToString());
        }
        catch (const std::exception &err)
        {
            std::cerr << "error writing to parquet: " << err.what() << '\n';
            return 1;
        }
    }

    if (global_flags.listen)
    {
        try
        {
            arrow::Status _s = send_tables(finished_tables);
            if (!_s.ok())
                throw std::runtime_error(_s.ToString());
        }
        catch (const std::exception &err)
        {
            std::cerr << "error sending data: " << err.what() << '\n';
            return 1;
        }
    }

    return 0;
}

arrow::Status process_sstable(std::shared_ptr<sstable_t> sstable)
{
    std::thread data(read_data, sstable);
    std::thread index(read_sstable_file<sstable_index_t>, sstable->index_path, &sstable->index);
    std::thread summary(read_sstable_file<sstable_summary_t>, sstable->summary_path, &sstable->summary);

    data.join();
    index.join();
    summary.join();

    if (sstable->statistics == nullptr || sstable->data == nullptr || sstable->index == nullptr || sstable->summary == nullptr)
        return arrow::Status::Invalid("Failed parsing SSTable");

    return arrow::Status::OK();
}

// ==================== READ SSTABLE FILES ====================
// These functions use kaitai to parse sstable files into C++
// objects.

void read_data(std::shared_ptr<sstable_t> sstable)
{
    try
    {
        read_sstable_file(sstable->statistics_path, &sstable->statistics);
        process_serialization_header(get_serialization_header(sstable->statistics));
        DEBUG_ONLY(debug_statistics(sstable->statistics));

        auto start_ts = std::chrono::high_resolution_clock::now();
        auto start = std::chrono::time_point_cast<std::chrono::microseconds>(start_ts).time_since_epoch().count();

        if (sstable->compression_info_path != "") // if the SSTable is compressed
        {
            read_sstable_file(sstable->compression_info_path, &sstable->compression_info);
            sstable->data = read_decompressed_sstable(sstable->compression_info, sstable->data_path);
        }
        else
            read_sstable_file(sstable->data_path, &sstable->data);

        auto end_ts = std::chrono::high_resolution_clock::now();
        auto end = std::chrono::time_point_cast<std::chrono::microseconds>(end_ts).time_since_epoch().count();

        std::cout << "[PROFILE read_data]: " << (end - start) << "us\n";
    }
    catch (const std::exception &err)
    {
        sstable->data = nullptr;
        return;
    }
}

std::shared_ptr<sstable_data_t> read_decompressed_sstable(std::shared_ptr<sstable_compression_info_t> compression_info, const boost::filesystem::path &data_path)
{
    std::ifstream ifs;
    try
    {
        open_stream(data_path, &ifs);
    }
    catch (const std::exception &e)
    {
        return nullptr;
    }

    // get the number of compressed bytes
    ifs.seekg(0, std::ios::end);
    int32_t src_size = static_cast<int32_t>(ifs.tellg()); // the total size of the uncompressed file
    std::cout << "compressed size: " << src_size << "\n";
    if (src_size < 0)
        throw std::runtime_error("error reading compressed data file");
    uint32_t nchunks = compression_info->chunk_count();

    std::vector<char> decompressed;

    const auto &offsets = *compression_info->chunk_offsets();
    uint32_t chunk_length = compression_info->chunk_length();

    uint64_t total_decompressed_size = 0;
    // loop through chunks to get total decompressed size (written by Cassandra)
    for (int i = 0; i < nchunks; ++i)
    {
        ifs.seekg(offsets[i]);
        int32_t decompressed_size =
            ((ifs.get() & 0xff) << 0x00) |
            ((ifs.get() & 0xff) << 0x08) |
            ((ifs.get() & 0xff) << 0x10) |
            ((ifs.get() & 0xff) << 0x18);
        total_decompressed_size += decompressed_size;
    }
    std::cout << "total decompressed size: " << total_decompressed_size << '\n';
    decompressed.resize(total_decompressed_size);

    for (int i = 0; i < nchunks; ++i) // read each chunk at a time
    {
        uint64_t offset = offsets[i];
        // get size based on offset with special case for last chunk
        uint64_t chunk_size = (i == nchunks - 1 ? src_size : offsets[i + 1]) - offset;

        std::vector<char> buffer(chunk_size);

        // skip 4 bytes written by Cassandra at beginning of each chunk and 4
        // bytes at end for Adler32 checksum
        ifs.seekg(offset + 4, std::ios::beg);
        ifs.read(buffer.data(), chunk_size - 8);

        int ntransferred = LZ4_decompress_safe(
            buffer.data(), &decompressed[i * chunk_length],
            chunk_size - 8, chunk_length);

        if (ntransferred < 0)
            throw std::runtime_error("decompression of block " + std::to_string(i) + " failed with error code " + std::to_string(ntransferred));
    }

    std::string decompressed_data(decompressed.data(), decompressed.size());
    kaitai::kstream ks(decompressed_data);
    return std::make_shared<sstable_data_t>(&ks);
}

template <typename T>
void read_sstable_file(const boost::filesystem::path &path, std::shared_ptr<T> *sstable_obj)
{
    std::ifstream ifs;
    try
    {
        open_stream(path, &ifs);
        kaitai::kstream ks(&ifs);
        *sstable_obj = std::make_shared<T>(&ks);
    }
    catch (const std::exception &err)
    {
        *sstable_obj = nullptr;
    }
}

// overload for statistics file, which requires the stream to persist
template <>
void read_sstable_file(const boost::filesystem::path &path, std::shared_ptr<sstable_statistics_t> *sstable_obj)
{
    // These streams are static because kaitai "instances" (a section of the
    // binary file specified by an offset) are lazy and will only read when the
    // value is accessed, and we still need to access these instances outside of
    // this function in order to read the serialization header data.
    static std::ifstream ifs;
    try
    {
        open_stream(path, &ifs);
        static kaitai::kstream ks(&ifs);
        *sstable_obj = std::make_shared<sstable_statistics_t>(&ks);
    }
    catch (const std::exception &err)
    {
        *sstable_obj = nullptr;
    }
}

// ==================== DEBUG SSTABLE FILES ====================
// These functions print some important data retrieved from the
// sstable files.

void debug_statistics(std::shared_ptr<sstable_statistics_t> statistics)
{
    auto body = get_serialization_header(statistics);
    std::cout << "\npartition key type: " << body->partition_key_type()->body() << '\n';

    std::cout
        << "min ttl: " << body->min_ttl()->val() << '\n'
        << "min timestamp: " << body->min_timestamp()->val() << '\n'
        << "min local deletion time: " << body->min_local_deletion_time()->val() << '\n';

    std::cout << "\n=== clustering keys (" << body->clustering_key_types()->length()->val() << ") ===\n";
    int i = 0;
    for (auto &type : *body->clustering_key_types()->array())
        std::cout << "type: " << type->body() << '\n';

    std::cout << "\n=== static columns (" << body->static_columns()->length()->val() << ") ===\n";
    i = 0;
    for (auto &column : *body->static_columns()->array())
        std::cout << column->name()->body() << "\t| " << column->column_type()->body() << '\n';

    std::cout << "\n=== regular columns (" << body->regular_columns()->length()->val() << ") ===\n";
    i = 0;
    for (auto &column : *body->regular_columns()->array())
        std::cout << column->name()->body() << "\t| " << column->column_type()->body() << '\n';
}

void debug_data(std::shared_ptr<sstable_data_t> sstable)
{
    for (auto &partition : *sstable->partitions())
    {
        std::cout << "\n========== partition ==========\nkey: " << partition->header()->key() << '\n';

        for (auto &unfiltered : *partition->unfiltereds())
        {
            if ((unfiltered->flags() & 0x01) != 0)
                break;

            if ((unfiltered->flags() & 0x02) != 0) // range tombstone marker
            {
                std::cout << "range tombstone marker\n";
                sstable_data_t::range_tombstone_marker_t *marker = (sstable_data_t::range_tombstone_marker_t *)unfiltered->body();
            }
            else
            {
                std::cout << "\n=== row ===\n";
                sstable_data_t::row_t *row = (sstable_data_t::row_t *)unfiltered->body();
                for (auto &cell : *row->clustering_blocks()->values())
                    std::cout << "clustering cell: " << cell << '\n';

                for (int i = 0; i < deserialization_helper_t::get_n_cols(deserialization_helper_t::REGULAR); ++i)
                {
                    if (deserialization_helper_t::is_multi_cell(deserialization_helper_t::REGULAR, i))
                    {
                        std::cout << "=== complex cell ===\n";
                        sstable_data_t::complex_cell_t *cell = (sstable_data_t::complex_cell_t *)(*row->cells())[i].get();
                        for (const auto &simple_cell : *cell->simple_cells())
                            std::cout << "child path, value as string: " << simple_cell->path()->value() << " | " << simple_cell->value() << '\n';
                    }
                    else
                    {
                        sstable_data_t::simple_cell_t *cell = (sstable_data_t::simple_cell_t *)(*row->cells())[i].get();
                        std::cout << "=== simple cell value as string: " << cell->value() << " ===\n";
                    }
                }
            }
        }
    }
    std::cout << "done reading data\n\n";
}

void debug_index(std::shared_ptr<sstable_index_t> index)
{
    for (std::unique_ptr<sstable_index_t::index_entry_t> &entry : *index->entries())
    {
        std::cout
            << "key: " << entry->key() << '\n'
            << "position: " << entry->position()->val() << '\n';

        if (entry->promoted_index_length()->val() > 0)
            std::cout << "promoted index exists\n";
    }
}

void debug_summary(std::shared_ptr<sstable_summary_t> summary)
{
    std::cout << "TODO implement summary debugger\n";
}

constexpr int MAX_FILEPATH_SIZE = 45;

/**
 * @brief Get the file paths of each SSTable file in a folder
 * Iterates through the specified folder and reads the file names into sstable
 * structs that map the file type (e.g. statistics, data) to the file
 * path.
 * @param path the path to the folder containing the SSTable files
 */
void get_file_paths(const boost::filesystem::path &dir_path, std::map<int, std::shared_ptr<sstable_t>> &sstables)
{
    using namespace boost::filesystem;

    char fmt[5];
    uint32_t num;
    char ftype_[25];
    for (const directory_entry &file : directory_iterator(dir_path))
    {
        path p = file.path();
        if (p.extension() != ".db")
        {
            std::cout << "skipping unrecognized file " << p << '\n';
            continue;
        }

        assert(p.filename().size() < MAX_FILEPATH_SIZE);
        if (sscanf(p.filename().c_str(), "%2c-%d-big-%[^.]", fmt, &num, ftype_) != 3) // number of arguments filled
        {
            std::cerr << "Error reading formatted filename\n";
            continue;
        }
        std::string_view ftype(ftype_);

        if (sstables.count(num) == 0)
            sstables[num] = std::make_shared<sstable_t>();

        if (ftype == "Data")
            sstables[num]->data_path = p;
        else if (ftype == "Statistics")
            sstables[num]->statistics_path = p;
        else if (ftype == "Index")
            sstables[num]->index_path = p;
        else if (ftype == "Summary")
            sstables[num]->summary_path = p;
        else if (ftype == "CompressionInfo")
            sstables[num]->compression_info_path = p;
    }
}

// ==================== HELPER FUNCTIONS ====================

void open_stream(const boost::filesystem::path &path, std::ifstream *ifs)
{
    *ifs = std::ifstream(path.c_str(), std::ifstream::binary);
    if (!ifs->is_open())
        throw std::runtime_error("could not open file \"" + path.string() + '\"');
}

// Initialize the deserialization helper using the schema from the serialization
// header stored in the statistics file. This must be called before `read_data`.
void process_serialization_header(sstable_statistics_t::serialization_header_t *serialization_header)
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
