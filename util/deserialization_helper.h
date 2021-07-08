#ifndef DESERIALIZATION_HELPER_H_
#define DESERIALIZATION_HELPER_H_

#include <kaitai/kaitaistruct.h>
#include <kaitai/kaitaistream.h>
#include <arrow/api.h>
#include <cmath>
#include <vector>
#include <memory>
#include <string>
#include <assert.h>
#include <map>
#include <set>
#include <iostream>

#include "vint.h"
#include "timer.h"
#include "conversions.h"

class deserialization_helper_t : public kaitai::kstruct
{
public:
    static const int CLUSTERING = 0;
    static const int STATIC = 1;
    static const int REGULAR = 2;

    static int idx;
    static int curkind;

    static const std::vector<std::shared_ptr<std::vector<std::string>>> colkinds;

    static int get_n_clustering_cells(int block);
    static int get_n_blocks();

    static void set_n_cols(int kind, int n);
    static int get_n_cols(int kind);

    static std::string get_col_type(int kind, int i);
    static void set_col_type(int kind, int i, const std::string &val);

    deserialization_helper_t(kaitai::kstream *ks);

    static int set_static();
    static int set_regular();
    static int get_n_cols();
    static bool is_multi_cell();
    static bool is_multi_cell(const std::string &type);
    static bool is_multi_cell(int kind, int i);

    int get_col_size();

    static int inc();
};

#endif
