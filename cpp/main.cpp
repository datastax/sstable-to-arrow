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
    instrumentor::get().begin_session("main");

    read_options(argc, argv);

    if (argc < 2)
    {
        std::cerr << "must specify path to table directory\n";
        return 1;
    }

    sstable_map_t sstables;
    const std::string table_dir = argv[optind];
    get_file_paths(table_dir, sstables);

    for (auto it = sstables.begin(); it != sstables.end(); ++it)
    {
        DEBUG_ONLY(std::cout << "\n\n===== Reading SSTable #" << it->first << " =====\n");
        process_sstable(it->second);
    }

    instrumentor::get().end_session();
    return 0;
}

arrow::Status process_sstable(std::shared_ptr<struct sstable_t> sstable)
{
    PROFILE_FUNCTION;
    std::thread data(read_data, sstable);
    std::thread index(read_sstable_file<sstable_index_t>, sstable->index_path, &sstable->index);
    std::thread summary(read_sstable_file<sstable_summary_t>, sstable->summary_path, &sstable->summary);

    data.join();
    index.join();
    summary.join();

    std::shared_ptr<arrow::Table> table;
    std::shared_ptr<arrow::Schema> schema;
    ARROW_RETURN_NOT_OK(vector_to_columnar_table(sstable->statistics, sstable->data, &schema, &table));

    if ((flags & WRITE_PARQUET_FLAG) != 0)
        write_parquet(*table, arrow::default_memory_pool());

    if ((flags & NO_NETWORK_FLAG) == 0)
        ARROW_RETURN_NOT_OK(send_data(schema, table));

    return arrow::Status::OK();
}

// ==================== READ SSTABLE FILES ====================
// These functions use kaitai to parse sstable files into C++
// objects.

void read_data(std::shared_ptr<struct sstable_t> sstable)
{
    PROFILE_FUNCTION;
    read_sstable_file(sstable->statistics_path, &sstable->statistics);
    process_serialization_header(get_serialization_header(sstable->statistics));
    DEBUG_ONLY(debug_statistics(sstable->statistics));

    auto start_ts = std::chrono::high_resolution_clock::now();
    auto start = std::chrono::time_point_cast<std::chrono::microseconds>(start_ts).time_since_epoch().count();

    read_sstable_file(sstable->data_path, &sstable->data);

    auto end_ts = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::time_point_cast<std::chrono::microseconds>(end_ts).time_since_epoch().count();

    std::cout << "[PROFILE read_data]: " << (end - start) << "us\n";
}

template <typename T>
void read_sstable_file(const std::string &path, std::shared_ptr<T> *sstable_obj)
{
    PROFILE_FUNCTION;
    std::ifstream ifs;
    open_stream(path, &ifs);
    kaitai::kstream ks(&ifs);
    *sstable_obj = std::make_shared<T>(&ks);
}

// overload for statistics file, which requires the stream to persist
template <>
void read_sstable_file(const std::string &path, std::shared_ptr<sstable_statistics_t> *sstable_obj)
{
    PROFILE_FUNCTION;
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
    PROFILE_FUNCTION;
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
    PROFILE_FUNCTION;
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
    PROFILE_FUNCTION;
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
    PROFILE_FUNCTION;
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
    PROFILE_FUNCTION;
    DIR *table_dir = opendir(path.c_str());
    struct dirent *dent;
    char fmt[5];
    int num;
    char ftype_[10];
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
    }
    free(table_dir);
}

void open_stream(const std::string &path, std::ifstream *ifs)
{
    PROFILE_FUNCTION;
    *ifs = std::ifstream(path, std::ifstream::binary);
    if (!ifs->is_open())
    {
        std::cerr << "could not open file";
        exit(1);
    }
}

// Read the serialization header from the statistics file.
sstable_statistics_t::serialization_header_t *get_serialization_header(std::shared_ptr<sstable_statistics_t> statistics)
{
    PROFILE_FUNCTION;
    const auto &toc = *statistics->toc()->array();
    const auto &ptr = toc[3]; // 3 is the index of the serialization header in the table of contents in the statistics file
    return static_cast<sstable_statistics_t::serialization_header_t *>(ptr->body());
}

// Initialize the deserialization helper using the schema from the serialization
// header stored in the statistics file. This must be called before `read_data`.
void process_serialization_header(sstable_statistics_t::serialization_header_t *serialization_header)
{
    PROFILE_FUNCTION;
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
