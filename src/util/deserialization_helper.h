#ifndef DESERIALIZATION_HELPER_H_
#define DESERIALIZATION_HELPER_H_

#include <kaitai/kaitaistream.h>
#include <cmath>
#include <vector>
#include <string>

class deserialization_helper_t
{
public:
    static std::vector<std::string> clustering_types;
    static std::vector<std::string> static_types;
    static std::vector<std::string> regular_types;

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

    static std::string get_clustering_type(int i);
    static void set_clustering_type(int i, std::string val);
    static std::string get_static_type(int i);
    static void set_static_type(int i, std::string val);
    static std::string get_regular_type(int i);
    static void set_regular_type(int i, std::string val);
};

#endif
