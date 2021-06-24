meta:
  id: sstable_index
  endian: be

seq:
  - id: entries
    type: index_entry
    repeat: eos

types:
  index_entry:
    seq:
      - id: key_length
        type: u2
      - id: key
        size: key_length
      - id: position
        type: s1
      - id: promoted_index_length
        type: s1
      - id: promoted_index
        type: promoted_index
        size: promoted_index_length
  
  promoted_index:
    seq:
      - id: partition_header_length
        type: s1
      - id: deletion_time
        type: deletion_time
      - id: promoted_index_blocks_count
        type: s1
      - id: blocks
        type: promoted_index_block
        repeat: expr
        repeat-expr: promoted_index_blocks_count
      - id: offsets
        type: u4
        repeat: expr
        repeat-expr: promoted_index_blocks_count
  
  promoted_index_block:
    seq:
      - id: first_name
        type: clustering_prefix
      - id: last_name
        type: clustering_prefix
      - id: offset
        type: s1
      - id: delta_width
        type: s1
      - id: end_open_marker_present
        type: u1
      - id: end_open_marker
        type: deletion_time
        if: end_open_marker_present != 0
  
  clustering_prefix:
    seq:
      - id: kind
        type: u1
      - id: size
        type: u2
        if: kind != 4
      - id: clustering_blocks
        type: clustering_block
        repeat: expr
        repeat-expr: _root.replace

  clustering_block:
    seq:
      - id: clustering_block_header
        type: s1
      - id: clustering_cells
        type: cell_value
        repeat: expr
        repeat-expr: _root.replace
  
  cell_value:
    seq:
      - id: length
        type: s1
        if: _root.replace != 0
      - id: value
        size: length

  deletion_time:
    seq:
      - id: local_deletion_time
        type: u4
        
      - id: marked_for_delete_at
        type: u8

instances:
  replace:
    value: 1234
