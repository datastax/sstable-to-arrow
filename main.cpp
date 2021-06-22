#include "main.h"

typedef sstable_statistics_t::toc_entry_t *entry_ptr;
typedef std::vector<sstable_statistics_t::column_t *> column_vector;
typedef std::vector<sstable_statistics_t::string_type_t *> string_vector;
typedef std::vector<sstable_index_t::index_entry_t *> entry_vector;

int main()
{
    // need to run this to initialize helper
    read_statistics("data2/store/shopping_cart-299c6500d32811ebb0629116fc548b6b/md-1-big-Statistics.db");

    read_data("./data/test_keyspace/test_table-8ae9e630d2aa11ebbcd94b7a475a76be/md-1-big-Data.db");

    // read_index("data3/store/shopping_cart-af4e4060d36911ebb9469116fc548b6b/md-1-big-Index.db");
    return 0;
}

void read_index(std::string path)
{
    std::ifstream ifs(path, std::ifstream::binary);
    kaitai::kstream ks(&ifs);
    sstable_index_t table_index(&ks);
    entry_vector *vec = table_index.entries();
    for (entry_vector::iterator it = vec->begin(); it != vec->end(); ++it)
    {
        sstable_index_t::index_entry_t *entry = *it;
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
}

void read_statistics(std::string path)
{
    std::ifstream statistics_ifs(path, std::ifstream::binary);
    kaitai::kstream ks(&statistics_ifs);
    sstable_statistics_t statistics(&ks);
    sstable_statistics_t::toc_t *toc = statistics.toc();

    std::vector<entry_ptr> *arr = toc->array();

    entry_ptr ptr = (*arr)[3];

    ks.seek(ptr->offset());

    sstable_statistics_t::serialization_header_t body(&ks);

    std::cout << "========== clustering keys ==========\n";

    string_vector *clustering_key_types = body.clustering_key_types()->array();
    for (string_vector::iterator it = clustering_key_types->begin(); it != clustering_key_types->end(); ++it)
    {
        sstable_statistics_t::string_type_t *clustering_key_type = *it;
        std::cout << clustering_key_type->body() << "\n";
    }

    return;

    std::cout
        << "partition key length: " << body.partition_key_type()->length()->val() << "\n";
    column_vector arr2 = *body.regular_columns()->array();
    std::cout << "array size: " << arr2.size() << "\n";
    for (column_vector::iterator it = arr2.begin(); it != arr2.end(); ++it)
    {
        sstable_statistics_t::column_t *col = *it;
        sstable_statistics_t::string_type_t *col_type = col->column_type();
        std::cout << col_type->body() << '\n';
    }

    deserialization_helper_t::set_n_clustering_cells(body.clustering_key_types()->length()->val());

    deserialization_helper_t::set_n_regular_columns(body.regular_columns()->length()->val());
    deserialization_helper_t::set_n_regular_columns(body.static_columns()->length()->val());

    column_vector *regular_columns = body.regular_columns()->array();
    for (column_vector::iterator it = regular_columns->begin(); it != regular_columns->end(); ++it)
    {
        std::cout << "name, type = " << (*it)->name()->body() << ", " << (*it)->column_type()->body() << '\n';
    }
    column_vector *static_columns = body.static_columns()->array();
    for (column_vector::iterator it = static_columns->begin(); it != static_columns->end(); ++it)
    {
        std::cout << "name, type = " << (*it)->name()->body() << ", " << (*it)->column_type()->body() << '\n';
    }
}

void read_data(std::string path)
{
    std::ifstream ifs(path, std::ifstream::binary);
    kaitai::kstream ks(&ifs);
    sstable_t sstable(&ks);
}
