#ifndef CLUSTERING_BLOCKS_H_
#define CLUSTERING_BLOCKS_H_

// This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

#include "kaitai/kaitaistruct.h"
#include <stdint.h>
#include "deserialization_helper.h"
#include "vint.h"
#include <vector>

#if KAITAI_STRUCT_VERSION < 9000L
#error "Incompatible Kaitai Struct C++/STL API: version 0.9 or later is required"
#endif
class deserialization_helper_t;
class vint_t;

class clustering_blocks_t : public kaitai::kstruct {

public:
    class clustering_block_t;
    class cell_value_t;

    clustering_blocks_t(kaitai::kstream* p__io, kaitai::kstruct* p__parent = 0, clustering_blocks_t* p__root = 0);

private:
    void _read();
    void _clean_up();

public:
    ~clustering_blocks_t();

    class clustering_block_t : public kaitai::kstruct {

    public:

        clustering_block_t(kaitai::kstream* p__io, clustering_blocks_t* p__parent = 0, clustering_blocks_t* p__root = 0);

    private:
        void _read();
        void _clean_up();

    public:
        ~clustering_block_t();

    private:
        vint_t* m_clustering_block_header;
        std::vector<cell_value_t*>* m_clustering_cells;
        clustering_blocks_t* m__root;
        clustering_blocks_t* m__parent;

    public:

        /**
         * Contains two bits per cell to encode if it is null, empty, or otherwise
         */
        vint_t* clustering_block_header() const { return m_clustering_block_header; }

        /**
         * Handles blocks of 32 cells
         * See ClusteringPrefix.Serializer.deserializeValuesWithoutSize https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L338
         */
        std::vector<cell_value_t*>* clustering_cells() const { return m_clustering_cells; }
        clustering_blocks_t* _root() const { return m__root; }
        clustering_blocks_t* _parent() const { return m__parent; }
    };

    class cell_value_t : public kaitai::kstruct {

    public:

        cell_value_t(kaitai::kstream* p__io, clustering_blocks_t::clustering_block_t* p__parent = 0, clustering_blocks_t* p__root = 0);

    private:
        void _read();
        void _clean_up();

    public:
        ~cell_value_t();

    private:
        std::string m_filler;
        vint_t* m_length;
        bool n_length;

    public:
        bool _is_null_length() { length(); return n_length; };

    private:
        std::string m_value;
        clustering_blocks_t* m__root;
        clustering_blocks_t::clustering_block_t* m__parent;

    public:
        std::string filler() const { return m_filler; }
        vint_t* length() const { return m_length; }
        std::string value() const { return m_value; }
        clustering_blocks_t* _root() const { return m__root; }
        clustering_blocks_t::clustering_block_t* _parent() const { return m__parent; }
    };

private:
    deserialization_helper_t* m_deserialization_helper;
    std::vector<clustering_block_t*>* m_clustering_blocks;
    clustering_blocks_t* m__root;
    kaitai::kstruct* m__parent;

public:
    deserialization_helper_t* deserialization_helper() const { return m_deserialization_helper; }

    /**
     * Only in non-static rows (does not appear if row is static)
     * See https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/Clustering.java#L141
     * Which leads to https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L293
     */
    std::vector<clustering_block_t*>* clustering_blocks() const { return m_clustering_blocks; }
    clustering_blocks_t* _root() const { return m__root; }
    kaitai::kstruct* _parent() const { return m__parent; }
};

#endif  // CLUSTERING_BLOCKS_H_
