# Kaitai Struct declaration for SSTable
# See https://thelastpickle.com/blog/2016/03/04/introductiont-to-the-apache-cassandra-3-storage-engine.html
meta:
  id: sstable
  endian: be
  ks-opaque-types: true # the only opaque type used is `vint` for Cassandra's variable integer encoding
  imports:
    - deletion_time

seq:
  - id: deserialization_helper
    type: deserialization_helper
  - id: partitions
    type: partition
    repeat: eos

doc: |
  "On disk a Partition consists of a header, followed by 1 or more Unfiltered objects." - The Last Pickle
  https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L364

types:
  partition:
    seq:
      - id: header
        type: partition_header

      # 0/1 static row, always goes first

      - id: unfiltereds # either row or range_tombstone_marker
        type: unfiltered
        repeat: until
        repeat-until: _.flags == 0x01 # end of partition
        doc: |
          Usually something that extends Unfiltered, which has Kind enum of either ROW or RANGE_TOMBSTONE_MARKER

  partition_header:
    seq:
      - id: key_length
        type: u2

      - id: key
        size: key_length
        doc: |
          Concatenated bytes of partition key columns

      - id: deletion_time
        type: deletion_time

  unfiltered:
    seq:
      - id: flags
        type: u1
      - id: body
        type:
          switch-on: flags
          cases:
            0x02: range_tombstone_marker
            _: row
        if: flags != 0x01
    doc: |
      Either a Row or a RangeTombstoneMarker

  empty:
    doc: |
      Intentionally blank.

  row:
    seq:
      - id: extended_flags # optional
        type: u1
        if: _parent.flags & 0x80 != 0 # EXTENSION_FLAG from https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L77
        doc: |
          Included if the EXTENSION_FLAG is set in the flags
          Always set for a static row or if there is a "shadowable" deletion

      - id: clustering_blocks # optional
        type: clustering_block
        repeat: expr
        repeat-expr: _root.deserialization_helper.get_n_blocks.as<u4>
        if: (_parent.flags & 0x80 == 0) or (extended_flags & 0x01 == 0) # no extended_flags or IS_STATIC flag is not set
        doc: |
          Only in non-static rows (does not appear if row is static)
          See https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/Clustering.java#L141
          Which leads to https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L293

      - id: row_body_size
        type: vint
        doc: |
          https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L270

      - id: previous_unfiltered_size
        type: vint

      - id: liveness_info
        type: liveness_info
        if: _parent.flags & 0x04 != 0 # HAS_TIMESTAMP set

      - id: deletion_time
        type: delta_deletion_time
        if: _parent.flags & 0x10 != 0 # HAS_DELETION set

      - id: columns_bitmask # optional
        type: columns_bitmask
        if: _parent.flags & 0x20 == 0 # HAS_ALL_COLUMNS not set
        doc: |
          Compare columns in this row to set of all columns in Memtable
          Encodes which columns missing when less than 64 columns; otherwise more complex

      - id: cells
        type: simple_cell # depends on if column is simple or complex
        repeat: expr
        repeat-expr: _root.deserialization_helper.get_n_columns.as<u4>

    doc: |
      See UnfilteredSerializer https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L30
      See line 125 for the actual implementation
      Can be static (no ClusteringPrefix) or non-static (has ClusteringPrefix)
      Static is always written first

  clustering_block:
    seq:
      - id: clustering_block_header
        type: vint
        doc: |
          Contains two bits per cell to encode if it is null, empty, or otherwise
      - id: clustering_cells
        type: simple_cell
        repeat: expr
        repeat-expr: _root.deserialization_helper.get_n_clustering_cells.as<u4> # TODO currently only works if less than 32 cells total
        doc: |
          Handles blocks of 32 cells
          See ClusteringPrefix.Serializer.deserializeValuesWithoutSize https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L338

  liveness_info:
    seq:
      - id: delta_timestamp
        type: vint
        doc: |
          Used to distinguish between a dead row (no live content cells, this primary key liveness info is empty)
          vs a live row where content cells are empty (no live content cells, but this primary key liveness info is NOT empty)
          If this is not empty, stores LivenessInfo.timestamp as delta from EncodingStats.minTimestamp

      - id: delta_ttl # optional
        type: vint
        if: _parent._parent.flags & 0x08 != 0 # HAS_TTL flag on `unfiltered`
        doc: |
          ExpiringLivenessInfo.ttl() is encoded as a variable sized integer delta from EncodingStats.minTTL

      - id: primary_key_liveness_deletion_time # optional
        type: vint
        if: _parent._parent.flags & 0x08 != 0 # HAS_TTL flag on `unfiltered`
        doc: |
          ExpiringLivenessInfo.localExpirationTime() is encoded as a variable sized integer delta from EncodingStats.minLocalDeletionTime

  delta_deletion_time:
    seq:
      - id: delta_marked_for_delete_at # optional
        type: vint
        doc: |
          DeletionTime.markedForDeleteAt() is encoded as a variable sized integer delta from EncodingStats.minTimestamp

      - id: delta_local_deletion_time # optional
        type: vint
        doc: |
          DeletionTime.localDeletionTime() is encoded as a variable sized integer delta from EncodingStats.minLocalDeletionTime.

  simple_cell:
    seq:
      - id: flags
        type: u1

      - id: delta_timestamp
        type: vint
        if: flags & 0x08 == 0 # USE_ROW_TIMESTAMP_MASK flag is off

      - id: delta_local_deletion_time
        type: vint
        if: (flags & 0x10 == 0) and ((flags & 0x01 != 0) or (flags & 0x02 != 0)) # if the cell does NOT use row TTL, and (the cell is deleted or it is expiring)

      - id: delta_ttl
        type: vint
        if: (flags & 0x10 == 0) and (flags & 0x02 != 0) # if cell does not use row TTL, and it is expiring

      - id: path
        type: cell_path
        if: false # TODO _parent.items_count

      - id: value
        type: cell_value
        if: flags & 0x04 == 0 # only if does not have empty value

    doc: |
      See https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L209
      for serialization info

  cell_path:
    seq:
      - id: length
        type: vint
      - id: value
        size: length.val.as<u4>

  cell_value:
    seq:
      - id: length
        type: vint
        if: true # TODO depends on schema definition
      - id: value
        size: length.val.as<u4> # TODO depends on schema definition

  # complex_cell:
  #   seq:
  #     - id: complex_deletion_time
  #       type: delta_deletion_time
  #       if: _parent.flags & 0x40 != 0 # HAS_COMPLEX_DELETION set on row
  #     - id: items_count
  #       type: vint
  #     - id: simple_cell
  #       type: simple_cell
  #       repeat: expr
  #       repeat-expr: items_count

  # ============================== RANGE TOMBSTONE MARKERS ==============================

  range_tombstone_marker:
    seq:
      - id: kind # stored as ordinal
        type: u1
      - id: bound_values_count
        type: u2
      - id: clustering_blocks
        type: clustering_block
        repeat: expr
        repeat-expr: _root.deserialization_helper.get_n_blocks.as<u4>
      - id: marker_body_size
        type: vint
      - id: previous_unfiltered_size
        type: vint
      - id: deletion_time
        type: delta_deletion_time
        if: kind == 0 or kind == 1 or kind == 6 or kind == 7
      - id: end_deletion_time
        type: delta_deletion_time
        if: kind == 2 or kind == 5
      - id: start_deletion_time
        type: delta_deletion_time
        if: kind == 2 or kind == 5
    doc: |
      See https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/RangeTombstone.java#L207
      for RangeTombstoneBound deserialization

  range_tombstone_body:
    seq:
      - id: range_tombstone_bound_clustering_block
        type: clustering_block

      - id: row_body_size
        type: vint
      - id: previous_row_body_size
        type: vint

      - id: boundary_end_deletion_marked_for_deletion_at # optional
        type: vint
        doc: |
          delta
      - id: boundary_end_deletion_local_deletion_time # optional
        type: vint
        doc: |
          delta
      - id: boundary_start_deletion_marked_for_deletion_at # optional
        type: vint
        doc: |
          delta
      - id: boundary_start_deletion_local_deletion_time # optional
        type: vint
        doc: |
          delta

    doc: |
      See https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L338

      See https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L220
      for serialization info
