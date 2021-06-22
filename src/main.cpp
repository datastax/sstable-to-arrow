#include "main.h"

typedef sstable_statistics_t::toc_entry_t *entry_ptr;
typedef std::vector<sstable_statistics_t::column_t *> column_vector;
typedef std::vector<sstable_statistics_t::string_type_t *> string_vector;
typedef std::vector<sstable_index_t::index_entry_t *> entry_vector;

int main()
{
    // specify these paths relative to project
    // need to run this to initialize helper
    read_statistics("data5/store/shopping_cart-c7c15ff0d39c11eb969ed7ee8570c286/md-1-big-Statistics.db");
    // read_index("data3/store/shopping_cart-af4e4060d36911ebb9469116fc548b6b/md-1-big-Index.db");
    read_data("data5/store/shopping_cart-c7c15ff0d39c11eb969ed7ee8570c286/md-1-big-Data.db");
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
    std::ifstream ifs(path, std::ifstream::binary);
    kaitai::kstream ks(&ifs);
    sstable_statistics_t statistics(&ks);
    entry_ptr ptr = (*statistics.toc()->array())[3];
    ks.seek(ptr->offset());
    sstable_statistics_t::serialization_header_t body(&ks);

    std::cout << "partition key type: " << body.partition_key_type()->body() << "\n";

    std::cout << "========== clustering keys ========== " << body.clustering_key_types()->length()->val() << "\n";
    string_vector *clustering_key_types = body.clustering_key_types()->array();
    for (string_vector::iterator it = clustering_key_types->begin(); it != clustering_key_types->end(); ++it)
    {
        std::cout << "clustering keys: " << (*it)->body() << "\n";
    }
    deserialization_helper_t::set_n_clustering_cells(body.clustering_key_types()->length()->val());

    std::cout << "=== static columns ===\n";
    column_vector *static_columns = body.static_columns()->array();
    for (column_vector::iterator it = static_columns->begin(); it != static_columns->end(); ++it)
    {
        std::cout << "name, type = " << (*it)->name()->body() << ", " << (*it)->column_type()->body() << '\n';
    }
    deserialization_helper_t::set_n_static_columns(body.static_columns()->length()->val());

    std::cout << "=== regular columns ===\n";
    column_vector *regular_columns = body.regular_columns()->array();
    for (column_vector::iterator it = regular_columns->begin(); it != regular_columns->end(); ++it)
    {
        std::cout << "name, type = " << (*it)->name()->body() << ", " << (*it)->column_type()->body() << '\n';
    }
    deserialization_helper_t::set_n_regular_columns(body.regular_columns()->length()->val());
}

void read_data(std::string path)
{
    std::ifstream ifs(path, std::ifstream::binary);
    kaitai::kstream ks(&ifs);
    sstable_data_t sstable(&ks);
}
