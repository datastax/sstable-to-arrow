#ifndef DESERIALIZATION_HELPER_H_
#define DESERIALIZATION_HELPER_H_

#include <kaitai/kaitaistruct.h> // for kstruct
#include <memory>                // for shared_ptr
#include <stdint.h>              // for uint64_t
#include <string>                // for string
#include <vector>                // for vector
namespace kaitai
{
class kstream;
} // namespace kaitai

class deserialization_helper_t : public kaitai::kstruct
{
  public:
    deserialization_helper_t(kaitai::kstream *ks);

    // returns the number of bytes that a value of this current column takes up
    // may read a varint
    int get_col_size();

    static const int CLUSTERING = 0;
    static const int STATIC = 1;
    static const int REGULAR = 2;

    // the index of the current cell out of the superset of columns in this
    // SSTable
    static int idx;
    // one of CLUSTERING, STATIC, REGULAR
    static int curkind;

    static const std::vector<std::shared_ptr<std::vector<std::string>>> colkinds;

    // a bitmask storing the missing columns
    static uint64_t bitmask;
    static int clear_bitmask();
    static void set_bitmask(uint64_t bitmask);

    static int get_n_clustering_cells(int block);
    static int get_n_blocks();

    static void set_n_cols(int kind, int n);
    static int get_n_cols(int kind);

    static std::string get_col_type(int kind, int i);
    static void set_col_type(int kind, int i, const std::string &val);

    static int set_static();
    static int set_regular();
    static int get_n_cells_in_row();

    static bool is_multi_cell();
    static bool is_multi_cell(const std::string &type);
    static bool is_multi_cell(int kind, int i);

    static int inc();
    void _read();
};

#endif
