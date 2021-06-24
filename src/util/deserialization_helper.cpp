#include "deserialization_helper.h"

int deserialization_helper_t::n_clustering_cells = -1;
int deserialization_helper_t::n_regular_columns = -1;
int deserialization_helper_t::n_static_columns = -1;

std::vector<std::string> deserialization_helper_t::clustering_types = {};
std::vector<std::string> deserialization_helper_t::static_types = {};
std::vector<std::string> deserialization_helper_t::regular_types = {};

// we don't actually want to read any bytes
deserialization_helper_t::deserialization_helper_t(kaitai::kstream *ks)
{
}

void deserialization_helper_t::set_n_clustering_cells(int n)
{
    n_clustering_cells = n;
    clustering_types.resize(n);
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
    regular_types.resize(n);
}

void deserialization_helper_t::set_n_static_columns(int n)
{
    n_static_columns = n;
    static_types.resize(n);
}

int deserialization_helper_t::get_n_columns()
{
    return n_regular_columns + n_static_columns;
}

std::string deserialization_helper_t::get_clustering_type(int i)
{
    return clustering_types[i];
}
void deserialization_helper_t::set_clustering_type(int i, std::string val)
{
    clustering_types[i] = val;
}

std::string deserialization_helper_t::get_static_type(int i)
{
    return static_types[i];
}
void deserialization_helper_t::set_static_type(int i, std::string val)
{
    static_types[i] = val;
}

std::string deserialization_helper_t::get_regular_type(int i)
{
    return regular_types[i];
}
void deserialization_helper_t::set_regular_type(int i, std::string val)
{
    regular_types[i] = val;
}
