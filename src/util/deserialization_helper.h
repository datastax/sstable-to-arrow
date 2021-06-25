#ifndef DESERIALIZATION_HELPER_H_
#define DESERIALIZATION_HELPER_H_

#include <kaitai/kaitaistream.h>
#include <cmath>
#include <vector>
#include <memory>
#include <string>
#include <assert.h>
#include <map>
#include <set>
#include "vint.h"

extern const std::map<std::string, int> is_fixed_len;
extern const std::set<std::string> is_multi_cell;

class deserialization_helper_t
{
    int idx;
    int curkind;
    kaitai::kstream *ks;

public:
    static const int CLUSTERING = 0;
    static const int STATIC = 1;
    static const int REGULAR = 2;

    static const std::vector<std::shared_ptr<std::vector<std::string>>> colkinds;

    static int get_n_clustering_cells(int block);
    static int get_n_blocks();

    static void set_n_cols(int kind, int n);
    static int get_n_cols(int kind);

    static std::string get_col_type(int kind, int i);
    static void set_col_type(int kind, int i, std::string val);

    deserialization_helper_t(kaitai::kstream *ks);

    int set_clustering();
    int set_static();
    int set_regular();
    int get_n_cols();
    bool is_complex();
    int get_col_size();
    int inc();
};

#endif
