// This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

#include "clustering_blocks.h"

clustering_blocks_t::clustering_blocks_t(kaitai::kstream* p__io, kaitai::kstruct* p__parent, clustering_blocks_t* p__root) : kaitai::kstruct(p__io) {
    m__parent = p__parent;
    m__root = this;
    m_deserialization_helper = 0;
    m_clustering_blocks = 0;

    try {
        _read();
    } catch(...) {
        _clean_up();
        throw;
    }
}

void clustering_blocks_t::_read() {
    m_deserialization_helper = new deserialization_helper_t(m__io);
    int l_clustering_blocks = static_cast<uint32_t>(deserialization_helper()->get_n_blocks());
    m_clustering_blocks = new std::vector<clustering_block_t*>();
    m_clustering_blocks->reserve(l_clustering_blocks);
    for (int i = 0; i < l_clustering_blocks; i++) {
        m_clustering_blocks->push_back(new clustering_block_t(m__io, this, m__root));
    }
}

clustering_blocks_t::~clustering_blocks_t() {
    _clean_up();
}

void clustering_blocks_t::_clean_up() {
    if (m_deserialization_helper) {
        delete m_deserialization_helper; m_deserialization_helper = 0;
    }
    if (m_clustering_blocks) {
        for (std::vector<clustering_block_t*>::iterator it = m_clustering_blocks->begin(); it != m_clustering_blocks->end(); ++it) {
            delete *it;
        }
        delete m_clustering_blocks; m_clustering_blocks = 0;
    }
}

clustering_blocks_t::clustering_block_t::clustering_block_t(kaitai::kstream* p__io, clustering_blocks_t* p__parent, clustering_blocks_t* p__root) : kaitai::kstruct(p__io) {
    m__parent = p__parent;
    m__root = p__root;
    m_clustering_block_header = 0;
    m_clustering_cells = 0;

    try {
        _read();
    } catch(...) {
        _clean_up();
        throw;
    }
}

void clustering_blocks_t::clustering_block_t::_read() {
    m_clustering_block_header = new vint_t(m__io);
    int l_clustering_cells = static_cast<uint32_t>(_root()->deserialization_helper()->get_n_clustering_cells());
    m_clustering_cells = new std::vector<cell_value_t*>();
    m_clustering_cells->reserve(l_clustering_cells);
    for (int i = 0; i < l_clustering_cells; i++) {
        m_clustering_cells->push_back(new cell_value_t(m__io, this, m__root));
    }
}

clustering_blocks_t::clustering_block_t::~clustering_block_t() {
    _clean_up();
}

void clustering_blocks_t::clustering_block_t::_clean_up() {
    if (m_clustering_block_header) {
        delete m_clustering_block_header; m_clustering_block_header = 0;
    }
    if (m_clustering_cells) {
        for (std::vector<cell_value_t*>::iterator it = m_clustering_cells->begin(); it != m_clustering_cells->end(); ++it) {
            delete *it;
        }
        delete m_clustering_cells; m_clustering_cells = 0;
    }
}

clustering_blocks_t::cell_value_t::cell_value_t(kaitai::kstream* p__io, clustering_blocks_t::clustering_block_t* p__parent, clustering_blocks_t* p__root) : kaitai::kstruct(p__io) {
    m__parent = p__parent;
    m__root = p__root;
    m_length = 0;

    try {
        _read();
    } catch(...) {
        _clean_up();
        throw;
    }
}

void clustering_blocks_t::cell_value_t::_read() {
    m_filler = m__io->read_bytes(0);
    n_length = true;
    if (static_cast<uint32_t>(_root()->deserialization_helper()->does_type_require_length()) != 0) {
        n_length = false;
        m_length = new vint_t(m__io);
    }
    m_value = m__io->read_bytes(static_cast<uint32_t>(length()->val()));
}

clustering_blocks_t::cell_value_t::~cell_value_t() {
    _clean_up();
}

void clustering_blocks_t::cell_value_t::_clean_up() {
    if (!n_length) {
        if (m_length) {
            delete m_length; m_length = 0;
        }
    }
}
