# WIP

meta:
  id: sstable_summary
  endian: be

seq:
  - id: header
    type: summary_header
  - id: summary_entries
    type: summary_entries_block
  - id: first
    type: serialized_key
  - id: last
    type: serialized_key

types:
  summary_header:
    seq:
      - id: min_index_interval
        type: u4
      - id: entries_count
        type: u4
      - id: summary_entries_size
        type: u8
      - id: sampling_level
        type: u4
      - id: size_at_full_sampling
        type: u4

  summary_entries_block:
    seq:
      - id: offsets
        type: u4
        repeat: expr
        repeat-expr: _parent.header.entries_count
      - id: entries
        type: summary_entry(_index)
        repeat: expr
        repeat-expr: _parent.header.entries_count

  summary_entry:
    params:
      - id: index
        type: u4
    seq:
      - id: key
        size: "(index == _root.header.entries_count - 1 ? _root.header.summary_entries_size :  _parent.offsets[index + 1]) - _parent.offsets[index] - 8"
      - id: position
        type: u8

  serialized_key:
    seq:
      - id: length
        type: u4
      - id: key
        size: length      
