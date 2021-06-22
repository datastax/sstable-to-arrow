# WIP

meta:
  id: sstable_summary
  endian: be

seq:

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
        type: u32
        repeat: expr
        repeat-expr: _parent.summary_header.entries_count
      - id: entries
        type: summary_entry
        size: offsets[_index] - offsets[_index-1]
        repeat: expr
        repeat-expr: _parent.summary_header.entries_count

  summary_entry:
    seq:
      - id: key
        type: u1
        repeat: expr
        repeat-expr:
      - id: position
        type: u8
