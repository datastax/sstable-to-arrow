#ifndef DESERIALIZATION_HELPER_H_
#define DESERIALIZATION_HELPER_H_

#include <kaitai/kaitaistream.h>
#include <cmath>

class deserialization_helper_t
{
public:
    static int n_clustering_cells;
    static int n_regular_columns;
    static int n_static_columns;

    deserialization_helper_t(kaitai::kstream *ks);
    
    static void set_n_clustering_cells(int n);
    static int get_n_clustering_cells();
    static int get_n_clustering_cells(int block);
    static int get_n_blocks();

    static void set_n_regular_columns(int n);
    static void set_n_static_columns(int n);
    static int get_n_regular_columns();
    static int get_n_static_columns();
    static int get_n_columns();
};

#endif
