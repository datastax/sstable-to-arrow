#include "deserialization_helper.h"

int deserialization_helper_t::n_clustering_cells = -1;
int deserialization_helper_t::n_regular_columns = -1;
int deserialization_helper_t::n_static_columns = -1;

// we don't actually want to read any bytes
deserialization_helper_t::deserialization_helper_t(kaitai::kstream *ks)
{
}

void deserialization_helper_t::set_n_clustering_cells(int n)
{
    n_clustering_cells = n;
}

int deserialization_helper_t::get_n_blocks()
{
    return ceil((double)n_clustering_cells / 32.);
}

int deserialization_helper_t::get_n_clustering_cells()
{
    return n_clustering_cells;
}

int deserialization_helper_t::get_n_clustering_cells(int block)
{
    return std::min(n_clustering_cells - block * 32, 32);
}

void deserialization_helper_t::set_n_regular_columns(int n)
{
    n_regular_columns = n;
}

void deserialization_helper_t::set_n_static_columns(int n)
{
    n_static_columns = n;
}

int deserialization_helper_t::get_n_columns()
{
    return n_regular_columns + n_static_columns;
}
