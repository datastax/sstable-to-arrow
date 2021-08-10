/**
 * Note that we use "kind" to refer to one of "clustering", "static", or
 * "regular", while we use "type" to refer to the actual data type stored inside
 * a cell, e.g. org.apache.cassandra.db.marshal.DateType.
 */

#include "deserialization_helper.h"
#include "conversions.h"      // for get_col_size, is_multi_cell
#include "opts.h"             // for DEBUG_ONLY
#include <algorithm>          // for min
#include <cassert>            // for assert
#include <ext/alloc_traits.h> // for __alloc_traits<>::value_type

constexpr void CHECK_KIND(int kind)
{
    assert(kind >= 0 && kind < 3);
}

// =============== DEFINE STATIC FIELDS ===============

int deserialization_helper_t::idx = 0;
int deserialization_helper_t::curkind = 0;
uint64_t deserialization_helper_t::bitmask = 0;

const std::vector<std::shared_ptr<std::vector<std::string>>> deserialization_helper_t::colkinds{
    std::make_shared<std::vector<std::string>>(), std::make_shared<std::vector<std::string>>(),
    std::make_shared<std::vector<std::string>>()};

// =============== METHOD DECLARATIONS ===============

// we don't actually want to read any bytes from the file
deserialization_helper_t::deserialization_helper_t(kaitai::kstream *ks) : kaitai::kstruct(ks)
{
}

/** Get the number of clustering, static, or regular columns */
int deserialization_helper_t::get_n_cols(int kind)
{
    CHECK_KIND(kind);
    return colkinds[kind]->size();
}
/** Set the number of clustering, static, or regular columns (and allocate
 * memory if setting) */
void deserialization_helper_t::set_n_cols(int kind, int n)
{
    CHECK_KIND(kind);
    colkinds[kind]->resize(n);
}

/** Get the data type stored in this column */
std::string deserialization_helper_t::get_col_type(int kind, int i)
{
    CHECK_KIND(kind);
    return (*colkinds[kind])[i];
}
/** Set the data type stored in this column */
void deserialization_helper_t::set_col_type(int kind, int i, const std::string &val)
{
    CHECK_KIND(kind);
    (*colkinds[kind])[i] = val;
}

/**
 * In each row, clustering cells are split up into blocks of 32
 * @returns how many columns are in the `block`-th clustering block
 */
int deserialization_helper_t::get_n_clustering_cells(int block)
{
    return std::min(get_n_cols(CLUSTERING) - block * 32, 32);
}
/**
 * Gets the number of clustering blocks total. Each one is a group of 32
 * clustering cells Equivalent to (int)ceil((double)number_of_clustering_cells
 * / 32.) but I avoid working with floats
 */
int deserialization_helper_t::get_n_blocks()
{
    return (get_n_cols(CLUSTERING) + 31) / 32;
}
bool deserialization_helper_t::is_multi_cell(int kind, int i)
{
    return conversions::is_multi_cell(get_col_type(kind, i));
}
bool deserialization_helper_t::is_multi_cell()
{
    return is_multi_cell(curkind, idx);
}

/** =============== utility functions for the sstable_data.ksy file
 * =============== These are for when we need to evaluate certain portions
 * imperatively due to restrictions with Kaitai Struct
 */

void deserialization_helper_t::set_bitmask(uint64_t bitmask_)
{
    bitmask = bitmask_;
}

// Increment the index of this helper (hover over the next cell)
int deserialization_helper_t::inc()
{
    for (int i = idx + 1; i < get_n_cols(curkind); ++i)
    {
        // if this column is not missing
        if (!(bitmask & (1 << i)))
        {
            idx = i;
            break;
        }
    }
    return 0;
}

// Indicate that we are processing a static row
int deserialization_helper_t::set_static()
{
    DEBUG_ONLY("begin processing static row\n");
    curkind = STATIC;
    idx = -1;
    inc(); // skip any missing columns at start
    return 0;
}

// Indicate that we are processing a regular row
int deserialization_helper_t::set_regular()
{
    DEBUG_ONLY("begin processing regular row\n");
    curkind = REGULAR;
    idx = -1;
    inc(); // skip any missing columns at start
    return 0;
}

// get the number of columns stored in this row
int deserialization_helper_t::get_n_cells_in_row()
{
    int superset_size = get_n_cols(curkind);
    int total = superset_size;
    // if some columns are missing
    if (curkind == REGULAR && bitmask != 0)
        for (int mask = 0; mask < superset_size; ++mask)
            if (bitmask & (1 << mask))
                --total;
    return total;
}

// Get the size of the current cell value in bytes
int deserialization_helper_t::get_col_size()
{
    return conversions::get_col_size(get_col_type(curkind, idx), _io());
}
