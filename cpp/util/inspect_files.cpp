#include "inspect_files.h"

// ==================== DEBUG SSTABLE FILES ====================
// These functions print some important data retrieved from the
// sstable files.

void debug_statistics(std::shared_ptr<sstable_statistics_t> statistics)
{
    std::cout << "========== statistics file ==========\n";
    auto body = get_serialization_header(statistics);
    std::cout << "\npartition key type: " << body->partition_key_type()->body() << '\n';

    std::cout
        << "min ttl: " << body->min_ttl()->val() << '\n'
        << "min timestamp: " << body->min_timestamp()->val() << '\n'
        << "min local deletion time: " << body->min_local_deletion_time()->val() << '\n';

    std::cout << "\n=== clustering keys (" << body->clustering_key_types()->length()->val() << ") ===\n";
    for (auto &type : *body->clustering_key_types()->array())
        std::cout << "type: " << type->body() << '\n';

    std::cout << "\n=== static columns (" << body->static_columns()->length()->val() << ") ===\n";
    for (auto &column : *body->static_columns()->array())
        std::cout << column->name()->body() << "\t| " << column->column_type()->body() << '\n';

    std::cout << "\n=== regular columns (" << body->regular_columns()->length()->val() << ") ===\n";
    for (auto &column : *body->regular_columns()->array())
        std::cout << column->name()->body() << "\t| " << column->column_type()->body() << '\n';
}

void debug_data(std::shared_ptr<sstable_data_t> sstable)
{
    std::cout << "========== data file ==========\n";
    for (auto &partition : *sstable->partitions())
    {
        std::cout << "\n========== partition ==========\n"
                     "key: "
                  << partition->header()->key() << '\n';

        for (auto &unfiltered : *partition->unfiltereds())
        {
            if ((unfiltered->flags() & 0x01) != 0)
                break;

            if ((unfiltered->flags() & 0x02) != 0) // range tombstone marker
            {
                std::cout << "range tombstone marker\n";
                // sstable_data_t::range_tombstone_marker_t *marker = (sstable_data_t::range_tombstone_marker_t *)unfiltered->body();
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
    (void)index;
    std::cout << "========== index file ==========\n"
                 "a description of the index file has not yet been implemented\n";
}

void debug_summary(std::shared_ptr<sstable_summary_t> summary)
{
    (void)summary;
    std::cout << "========== summary file ==========\n"
                 "a description of the summary file has not yet been implemented\n";
}
