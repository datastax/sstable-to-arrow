#include "main.h"

using std::shared_ptr;
using std::unique_ptr;
using std::vector;

const char fpath_separator =
#ifdef _WIN32
    '\\';
#else
    '/';
#endif

const std::map<std::string, std::string> sstable_files = {
    {"data", "Data.db"},
    {"statistics", "Statistics.db"},
    {"index", "Index.db"}};

bool ends_with(std::string s, std::string end)
{
    if (end.size() > s.size())
        return false;
    return std::equal(end.rbegin(), end.rend(), s.rbegin());
}

void get_file_paths(const std::string path, std::map<std::string, std::string> &file_paths)
{
    DIR *table_dir = opendir(path.c_str());
    struct dirent *dent;
    while ((dent = readdir(table_dir)) != nullptr)
    {
        const std::string fname = dent->d_name;
        for (auto it = sstable_files.begin(); it != sstable_files.end(); ++it)
            if (ends_with(fname, it->second))
                file_paths[it->first] = path + fpath_separator + fname;
    }
}

const int NO_NETWORK_FLAG = 0x01;

int main(int argc, char *argv[])
{
    std::shared_ptr<sstable_index_t> index;
    std::shared_ptr<sstable_data_t> sstable;
    std::shared_ptr<sstable_statistics_t> statistics;
    std::shared_ptr<sstable_summary_t> summary;

    int flags = 0;
    int opt;
    while ((opt = getopt(argc, argv, ":t:m:i:n")) != -1)
    {
        switch (opt)
        {
        case 'm':
            read_summary(optarg, &summary);
            return 0;
        case 't':
            std::cout << "reading statistics\n";
            read_statistics(optarg, &statistics);
            return 0;
        case 'i':
            read_index(optarg, &index);
            return 0;
        case 'n': // turn off sending via network
            flags |= NO_NETWORK_FLAG;
            break;
        default:
            break;
        }
    }

    if (argc < 2)
    {
        perror("must specify path to table directory");
        return 1;
    }

    const std::string table_dir = argv[optind];
    std::map<std::string, std::string> file_paths;
    get_file_paths(table_dir, file_paths);

    for (auto it = sstable_files.begin(); it != sstable_files.end(); ++it)
        assert(file_paths.find(it->first) != file_paths.end());

    read_statistics(file_paths["statistics"], &statistics);
    read_data(file_paths["data"], &sstable);
    read_index(file_paths["index"], &index);

    if ((flags & NO_NETWORK_FLAG) == 0)
    {
        std::shared_ptr<arrow::Table> table;
        std::shared_ptr<arrow::Schema> schema;
        EXIT_ON_FAILURE(vector_to_columnar_table(statistics, sstable, &schema, &table));
        arrow::Status status = send_data(schema, table);
        if (!status.ok())
            return 1;
    }
    return 0;
}

void read_index(const std::string &path, std::shared_ptr<sstable_index_t> *index)
{
    std::ifstream ifs;
    open_stream(path, &ifs);
    kaitai::kstream ks(&ifs);
    *index = std::make_shared<sstable_index_t>(&ks);

    for (unique_ptr<sstable_index_t::index_entry_t> &entry : *index->get()->entries())
    {
        std::cout
            << "key: " << entry->key() << "\n"
            << "position: " << entry->position()->val() << "\n";

        if (entry->promoted_index_length()->val() > 0)
        {
            std::cout << "promoted index exists\n";
        }
    }
}

void read_statistics(const std::string &path, std::shared_ptr<sstable_statistics_t> *statistics)
{
    std::ifstream ifs;
    open_stream(path, &ifs);
    kaitai::kstream ks(&ifs);
    *statistics = std::make_shared<sstable_statistics_t>(&ks);

    auto &ptr = (*statistics->get()->toc()->array())[3];
    sstable_statistics_t::serialization_header_t *body = (sstable_statistics_t::serialization_header_t *)ptr->body();

    std::cout << "\npartition key type: " << body->partition_key_type()->body() << "\n";

    std::cout
        << "min ttl: " << body->min_ttl()->val() << '\n'
        << "min timestamp: " << body->min_timestamp()->val() << '\n'
        << "min local deletion time: " << body->min_local_deletion_time()->val() << '\n';

    int i;

    // Set important constants for the serialization helper and initialize vectors to store
    // types of clustering columns
    deserialization_helper_t::set_n_cols(deserialization_helper_t::CLUSTERING, body->clustering_key_types()->length()->val());
    deserialization_helper_t::set_n_cols(deserialization_helper_t::STATIC, body->static_columns()->length()->val());
    deserialization_helper_t::set_n_cols(deserialization_helper_t::REGULAR, body->regular_columns()->length()->val());

    std::cout << "\n=== clustering keys (" << body->clustering_key_types()->length()->val() << ") ===\n";
    i = 0;
    for (auto &type : *body->clustering_key_types()->array())
    {
        std::cout << "type: " << type->body() << "\n";
        deserialization_helper_t::set_col_type(deserialization_helper_t::CLUSTERING, i++, type->body());
    }

    std::cout << "\n=== static columns (" << body->static_columns()->length()->val() << ") ===\n";
    i = 0;
    for (auto &column : *body->static_columns()->array())
    {
        std::cout << column->name()->body() << "\t| " << column->column_type()->body() << '\n';
        deserialization_helper_t::set_col_type(deserialization_helper_t::STATIC, i++, column->column_type()->body());
    }

    std::cout << "\n=== regular columns (" << body->regular_columns()->length()->val() << ") ===\n";
    i = 0;
    for (auto &column : *body->regular_columns()->array())
    {
        std::cout << column->name()->body() << "\t| " << column->column_type()->body() << '\n';
        deserialization_helper_t::set_col_type(deserialization_helper_t::REGULAR, i++, column->column_type()->body());
    }
}

void read_data(const std::string &path, std::shared_ptr<sstable_data_t> *sstable)
{
    std::ifstream ifs;
    open_stream(path, &ifs);
    kaitai::kstream ks(&ifs);
    *sstable = std::make_shared<sstable_data_t>(&ks);

    for (auto &partition : *sstable->get()->partitions())
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
                {
                    std::cout << "clustering cell: " << cell << '\n';
                }

                for (auto &cell : *row->cells())
                {
                    sstable_data_t::simple_cell_t *simple_cell = (sstable_data_t::simple_cell_t *)cell.get();
                    std::cout << "cell value: " << simple_cell->value()->value() << "\n";
                }
            }
        }
    }
}

// TODO
void read_summary(const std::string &path, std::shared_ptr<sstable_summary_t> *summary)
{
    std::ifstream ifs;
    open_stream(path, &ifs);
    kaitai::kstream ks(&ifs);
    *summary = std::make_shared<sstable_summary_t>(&ks);
}

void open_stream(const std::string &path, std::ifstream *ifs)
{
    std::cout << "\n===== opening " << path << " =====\n";
    *ifs = std::ifstream(path, std::ifstream::binary);
    if (!ifs->is_open())
    {
        perror("could not open file");
        exit(1);
    }
}
