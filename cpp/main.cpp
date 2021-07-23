#include "main.h"

const char fpath_separator =
#ifdef _WIN32
    '\\';
#else
    '/';
#endif

const int NO_NETWORK_FLAG = 0x01;
const int WRITE_PARQUET_FLAG = 0x02;
int flags;

typedef std::map<int, std::shared_ptr<struct sstable_t>> sstable_map_t;

int main(int argc, char *argv[])
{
    read_options(argc, argv);

    if (argc < 2)
    {
        std::cerr << "must specify path to table directory\n";
        return 1;
    }

    sstable_map_t sstables;
    const std::string table_dir = argv[optind];
    get_file_paths(table_dir, sstables);

    std::vector<std::shared_ptr<arrow::Table>> finished_tables(sstables.size());

    int i = 0;
    for (auto it = sstables.begin(); it != sstables.end(); ++it)
    {
        std::cout << "\n\n========== Reading SSTable #" << it->first << " ==========\n";
        process_sstable(it->second);
        arrow::Status status = vector_to_columnar_table(it->second->statistics, it->second->data, &finished_tables[i++]);
        if (!status.ok())
        {
            std::cerr << "error converting SSTable data to Arrow Table\n";
            return 1;
        }
    }

    auto final_table_result = arrow::ConcatenateTables(finished_tables, arrow::ConcatenateTablesOptions{true});
    if (!final_table_result.ok())
    {
        std::cerr << "error concatenating sstables\n";
        return 1;
    }
    auto final_table = final_table_result.ValueOrDie();

    if ((flags & WRITE_PARQUET_FLAG) != 0)
    {
        if (!write_parquet(final_table).ok())
        {
            std::cerr << "error writing to parquet\n";
            return 1;
        }
    }

    if ((flags & NO_NETWORK_FLAG) == 0)
    {
        if (!send_tables(finished_tables).ok())
        {
            std::cerr << "error sending data\n";
            return 1;
        }
    }

    return 0;
}

arrow::Status process_sstable(std::shared_ptr<struct sstable_t> sstable)
{
    std::thread data(read_data, sstable);
    std::thread index(read_sstable_file<sstable_index_t>, sstable->index_path, &sstable->index);
    std::thread summary(read_sstable_file<sstable_summary_t>, sstable->summary_path, &sstable->summary);

    data.join();
    index.join();
    summary.join();

    return arrow::Status::OK();
}

// ==================== READ SSTABLE FILES ====================
// These functions use kaitai to parse sstable files into C++
// objects.

void read_data(std::shared_ptr<struct sstable_t> sstable)
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

std::shared_ptr<sstable_data_t> read_decompressed_sstable(std::shared_ptr<sstable_compression_info_t> compression_info, const std::string &data_path)
{
    std::ifstream ifs;
    open_stream(data_path, &ifs);

    // get the number of compressed bytes
    ifs.seekg(0, std::ios::end);
    int32_t src_size = static_cast<int32_t>(ifs.tellg()); // the total size of the uncompressed file
    std::cout << "compressed size: " << src_size << "\n";
    if (src_size < 0)
    {
        std::cerr << "error reading compressed data file\n";
        exit(1);
    }
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
        {
            std::cerr << "decompression of block " << i << " failed with error code " << ntransferred << '\n';
            exit(1);
        }
    }

    std::string decompressed_data(decompressed.data(), decompressed.size());
    kaitai::kstream ks(decompressed_data);
    return std::make_shared<sstable_data_t>(&ks);
}

template <typename T>
void read_sstable_file(const std::string &path, std::shared_ptr<T> *sstable_obj)
{
    std::ifstream ifs;
    open_stream(path, &ifs);
    kaitai::kstream ks(&ifs);
    *sstable_obj = std::make_shared<T>(&ks);
}

// overload for statistics file, which requires the stream to persist
template <>
void read_sstable_file(const std::string &path, std::shared_ptr<sstable_statistics_t> *sstable_obj)
{
    // These streams are static because kaitai "instances" (a section of the
    // binary file specified by an offset) are lazy and will only read when the
    // value is accessed, and we still need to access these instances outside of
    // this function in order to read the serialization header data.
    static std::ifstream ifs;
    open_stream(path, &ifs);
    static kaitai::kstream ks(&ifs);
    *sstable_obj = std::make_shared<sstable_statistics_t>(&ks);
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

// ==================== HELPER FUNCTIONS ====================

void read_options(int argc, char *argv[])
{
    int opt;
    std::shared_ptr<sstable_summary_t> summary;
    std::shared_ptr<sstable_statistics_t> statistics;
    std::shared_ptr<sstable_index_t> index;
    while ((opt = getopt(argc, argv, ":t:m:i:np")) != -1)
    {

        switch (opt)
        {
        case 'm':
            read_sstable_file(optarg, &summary);
            return;
        case 't':
            read_sstable_file(optarg, &statistics);
            return;
        case 'i':
            read_sstable_file(optarg, &index);
            return;
        case 'n': // turn off sending via network
            flags |= NO_NETWORK_FLAG;
            break;
        case 'p':
            flags |= WRITE_PARQUET_FLAG;
            break;
        default:
            break;
        }
    }
}

bool ends_with(const std::string &s, const std::string &end)
{
    if (end.size() > s.size())
        return false;
    return std::equal(end.rbegin(), end.rend(), s.rbegin());
}

/**
 * @brief Get the file paths of each SSTable file in a folder
 * Iterates through the specified folder and reads the file names into sstable
 * structs that map the file type (e.g. statistics, data) to the file
 * path.
 * @param path the path to the folder containing the SSTable files
 */
void get_file_paths(const std::string &path, sstable_map_t &sstables)
{
    DIR *table_dir = opendir(path.c_str());
    struct dirent *dent;
    char fmt[5];
    int num;
    char ftype_[25];
    while ((dent = readdir(table_dir)) != nullptr)
    {
        std::string fname = dent->d_name;
        if (!ends_with(fname, ".db"))
            continue;

        if (sscanf(dent->d_name, "%2c-%d-big-%[^.]", fmt, &num, ftype_) != 3) // number of arguments filled
        {
            std::cerr << "Error reading formatted filename\n";
            continue;
        }
        std::string ftype = ftype_;
        std::string full_path = path + fpath_separator + dent->d_name;

        if (sstables.count(num) == 0)
            sstables[num] = std::make_shared<sstable_t>();

        if (ftype == "Data")
            sstables[num]->data_path = full_path;
        else if (ftype == "Statistics")
            sstables[num]->statistics_path = full_path;
        else if (ftype == "Index")
            sstables[num]->index_path = full_path;
        else if (ftype == "Summary")
            sstables[num]->summary_path = full_path;
        else if (ftype == "CompressionInfo")
            sstables[num]->compression_info_path = full_path;
    }
    closedir(table_dir);
}

void open_stream(const std::string &path, std::ifstream *ifs)
{
    *ifs = std::ifstream(path, std::ifstream::binary);
    if (!ifs->is_open())
    {
        std::cerr << "could not open file \"" << path << "\"\n";
        exit(1);
    }
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
