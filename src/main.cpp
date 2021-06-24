#include "main.h"

using std::shared_ptr;
using std::unique_ptr;
using std::vector;

int main(int argc, char *argv[])
{
    // int opt;
    // char flags = 0x00;
    // while ((opt = getopt(argc, argv, ":s:d:")) != -1)
    // {
    //     switch (opt)
    //     {
    //     case 's':
    //         flags |= 0x01;
    //         break;

    //     default:
    //         break;
    //     }
    // }

    // if (optind >= argc)
    // {
    //     std::cout << "Please specify the path to the db directory containing the .db files\n";
    //     return 1;
    // }

    // if (access(argv[optind], F_OK) != 0)
    // {
    //     std::cout << "Database directory does not exist, please double-check your file path\n";
    // }

    // std::string dbpath(argv[optind]);

    // specify these paths relative to project
    // need to run this to initialize helper
    read_statistics("res/school/classes-3a387eb0d3e311eb95b1cf2bb105377c/md-1-big-Statistics.db");
    // read_index("data3/store/shopping_cart-af4e4060d36911ebb9469116fc548b6b/md-1-big-Index.db");
    sstable_data_t *sstable = read_data("res/school/classes-3a387eb0d3e311eb95b1cf2bb105377c/md-1-big-Data.db");

    // std::shared_ptr<arrow::Table> table;
    // std::shared_ptr<arrow::Schema> schema;
    // EXIT_ON_FAILURE(vector_to_columnar_table(sstable, &schema, &table));

    // send_data(schema, table);

    return 0;
}

sstable_index_t *read_index(std::string path)
{
    std::ifstream ifs(path, std::ifstream::binary);
    kaitai::kstream ks(&ifs);
    static sstable_index_t index(&ks);

    for (unique_ptr<sstable_index_t::index_entry_t> &entry : *index.entries())
    {
        std::cout
            << "key: " << entry->key() << "\n"
            << "position: " << entry->position()->val() << "\n";
        // if (entry->promoted_index_length()->val() > 0)
        // {
        // std::cout << "promoted index:\n";
        // std::cout
        //     // << entry->promoted_index()->blocks()
        // }
        // << "position: " << entry->promoted_index() << "\n"
        // << "position: " << entry->position() << "\n";
    }

    return &index;
}

sstable_statistics_t *read_statistics(std::string path)
{
    std::cout << "\n\n===== READING STATISTICS =====\n";

    std::ifstream ifs(path, std::ifstream::binary);
    kaitai::kstream ks(&ifs);
    static sstable_statistics_t statistics(&ks);

    auto &ptr = (*statistics.toc()->array())[3];
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
        std::cout
            << "name: " << column->name()->body() << "\n"
            << "type: " << column->column_type()->body() << '\n';
        deserialization_helper_t::set_col_type(deserialization_helper_t::STATIC, i++, column->name()->body());
    }

    std::cout << "\n=== regular columns (" << body->regular_columns()->length()->val() << ") ===\n";
    i = 0;
    for (auto &column : *body->regular_columns()->array())
    {
        std::cout
            << "name: " << column->name()->body() << "\n"
            << "type: " << column->column_type()->body() << '\n';
        deserialization_helper_t::set_col_type(deserialization_helper_t::REGULAR, i++, column->name()->body());
    }

    return &statistics;
}

sstable_data_t *read_data(std::string path)
{
    std::cout << "\n\n===== READING DATA =====\n";

    std::ifstream ifs(path, std::ifstream::binary);
    kaitai::kstream ks(&ifs);
    static sstable_data_t sstable(&ks);

    for (auto &partition : *sstable.partitions())
    {
        std::cout << "\n========== partition ==========\nkey: " << partition->header()->key() << '\n';

        for (auto &unfiltered : *partition->unfiltereds())
        {
            if ((unfiltered->flags() & 0x01) != 0)
                break;

            // u->body()->_io()
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

    return &sstable;
}
