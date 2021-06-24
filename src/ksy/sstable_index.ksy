meta:
  id: sstable_index
  endian: be
  ks-opaque-types: true
  imports:
    - deletion_time

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
        type: vint
      - id: promoted_index_length
        type: vint
      - id: promoted_index
        type: promoted_index
        size: promoted_index_length.val.as<u4>
        if: promoted_index_length.val.as<u4> > 0
  
  promoted_index:
    seq:
      - id: partition_header_length
        type: vint
      - id: deletion_time
        type: deletion_time
      - id: promoted_index_blocks_count
        type: vint
      - id: blocks
        type: promoted_index_block
        repeat: expr
        repeat-expr: promoted_index_blocks_count.val.as<u4>
      - id: offsets
        type: u4
        repeat: expr
        repeat-expr: promoted_index_blocks_count.val.as<u4>
  
  promoted_index_block:
    seq:
      - id: first_name
        type: clustering_prefix
      - id: last_name
        type: clustering_prefix
      - id: offset
        type: vint
      - id: delta_width
        type: vint
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
        type: clustering_blocks
  
  simple_cell:
    seq:
      - id: length
        type: vint
      - id: value
        size: length.val.as<u4>
