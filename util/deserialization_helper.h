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

struct cassandra_type
{
    std::string cql_name;
    size_t fixed_len;
    std::shared_ptr<arrow::DataType> arrow_type;
};

const std::string listtype = "org.apache.cassandra.db.marshal.ListType";
const std::string maptype = "org.apache.cassandra.db.marshal.MapType";
const std::string settype = "org.apache.cassandra.db.marshal.SetType";
const std::string tupletype = "org.apache.cassandra.db.marshal.TupleType"; // TODO

// order matters and is used in sstable_data.ksy to determine how child cells
// are encoded
const std::vector<std::string> multi_cell_types{
    listtype,
    maptype,
    settype};

extern const std::map<std::string, struct cassandra_type> type_info;
extern const std::set<std::string> is_multi_cell;

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
    int get_col_size(const std::string &type);

    static int inc();
};

#endif
