# Kaitai Struct declaration for SSTable
# See https://thelastpickle.com/blog/2016/03/04/introductiont-to-the-apache-cassandra-3-storage-engine.html

# note that this file uses a bit of hacking using `deserialization_helper` to access data about the schema,
# which gets loaded from `sstable_statistics.ksy`
# kaitai doesn't like if we try to access properties of an opaque type (one that's
# manually implemented outside of kaitai) unless we cast it, so we make the imperative `deserialization_helper` functions
# return 0 and set that to the size of a kaitai field with the id tmp_, tmp1_, etc.
# these fields are used only for getting the parser to work properly and do not contain any data

meta:
  id: sstable_data
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

      # optional static row always goes before regular rows, but it's very similar to
      # a regular row so we still do the parsing for it in the `row` type

      - id: unfiltereds # either row or range_tombstone_marker
        type: unfiltered
        repeat: until
        repeat-until: (_.flags & 0x01) != 0 # end of partition
        doc: Usually something that extends Unfiltered, i.e. either a Row or RangeTombstoneMarker

  partition_header:
    seq:
      - id: key_length
        type: u2
      - id: key
        size: key_length
        doc: Concatenated bytes of partition key columns
      - id: deletion_time
        type: deletion_time

  unfiltered:
    seq:
      # enum class row_flags {
      #     // Signal the end of the partition. Nothing follows a <flags> field with that flag.
      #     END_OF_PARTITION = 0x01,
      #     // Whether the encoded unfiltered is a marker or a row. All following flags apply only to rows.
      #     IS_MARKER = 0x02,
      #     // Whether the encoded row has a timestamp (i.e. its liveness_info is not empty).
      #     HAS_TIMESTAMP = 0x04,
      #     // Whether the encoded row has some expiration info (i.e. if its liveness_info contains TTL and local_deletion).
      #     HAS_TTL = 0x08,
      #     // Whether the encoded row has some deletion info.
      #     HAS_DELETION = 0x10,
      #     // Whether the encoded row has all of the columns from the header present.
      #     HAS_ALL_COLUMNS = 0x20,
      #     // Whether the encoded row has some complex deletion for at least one of its complex columns.
      #     HAS_COMPLEX_DELETION = 0x40,
      #     // If present, another byte is read containing the "extended flags" below.
      #     EXTENSION_FLAG = 0x80
      # };
      - id: flags
        type: u1
      - id: body
        type:
          switch-on: flags
          cases:
            0x02: range_tombstone_marker
            _: row
        if: (flags & 0x01) == 0
    doc: |
      Either a Row or a RangeTombstoneMarker

  row:
    seq:
      - id: extended_flags
        type: u1
        if: (_parent.flags & 0x80) != 0 # EXTENSION_FLAG from https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L77
        doc: |
          Included if the EXTENSION_FLAG is set in the flags
          Always set for a static row or if there is a "shadowable" deletion

      - id: clustering_blocks
        type: clustering_blocks
        if: ((_parent.flags & 0x80) == 0) or ((extended_flags & 0x01) == 0) # if row is not static

      - id: row_body_size
        type: vint
        doc: |
          https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L270

      - id: previous_unfiltered_size
        type: vint

      - id: liveness_info
        type: liveness_info
        if: (_parent.flags & 0x04) != 0 # HAS_TIMESTAMP set

      - id: deletion_time
        type: delta_deletion_time
        if: (_parent.flags & 0x10) != 0 # HAS_DELETION set

      - id: columns_bitmask
        type: columns_bitmask
        if: (_parent.flags & 0x20) == 0 # HAS_ALL_COLUMNS not set
        doc: |
          Compare columns in this row to set of all columns in Memtable
          Encodes which columns missing when less than 64 columns; otherwise more complex

      - id: tmp_
        size: "((_parent.flags & 0x80 == 0) or (extended_flags & 0x01 == 0)) ? _root.deserialization_helper.set_regular.as<u4> : _root.deserialization_helper.set_static.as<u4>"

      - id: cells
        type:
          switch-on: _root.deserialization_helper.is_complex.as<b1>
          cases:
            true: complex_cell
            false: simple_cell(false) # "false" means "not a child of a complex cell"
        repeat: expr
        repeat-expr: _root.deserialization_helper.get_n_cols.as<u4>

    doc: |
      See UnfilteredSerializer https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L30
      See line 125 for the actual implementation
      Can be static (no ClusteringPrefix) or non-static (has ClusteringPrefix)
      Static is always written first

  liveness_info:
    seq:
      - id: delta_timestamp
        type: vint
        doc: |
          Used to distinguish between a dead row (no live content cells, this primary key liveness info is empty)
          vs a live row where content cells are empty (no live content cells, but this primary key liveness info is NOT empty)
          If this is not empty, stores LivenessInfo.timestamp as delta from EncodingStats.minTimestamp

      - id: delta_ttl
        type: vint
        if: (_parent._parent.flags & 0x08) != 0 # HAS_TTL flag on `unfiltered`
        doc: delta from EncodingStats.minTTL

      - id: primary_key_liveness_deletion_time
        type: vint
        if: (_parent._parent.flags & 0x08) != 0 # HAS_TTL flag on `unfiltered`
        doc: delta from EncodingStats.minLocalDeletionTime

  delta_deletion_time:
    seq:
      - id: delta_marked_for_delete_at
        type: vint
        doc: delta from EncodingStats.minTimestamp

      - id: delta_local_deletion_time
        type: vint
        doc: delta from EncodingStats.minLocalDeletionTime.

  simple_cell:
    params:
      - id: complex
        type: b1
    seq:
      - id: flags
        type: u1

      - id: delta_timestamp
        type: vint
        if: (flags & 0x08) == 0 # USE_ROW_TIMESTAMP_MASK flag is off

      - id: delta_local_deletion_time
        type: vint
        if: ((flags & 0x10) == 0) and (((flags & 0x01) != 0) or ((flags & 0x02) != 0)) # if the cell does NOT use row TTL, and (the cell is deleted or it is expiring)

      - id: delta_ttl
        type: vint
        if: ((flags & 0x10) == 0) and ((flags & 0x02) != 0) # if cell does not use row TTL, and it is expiring

      - id: path
        type: cell_path
        if: complex

        # TODO maybe process this differently directly into arrow instead of processing it after?
        # doesn't change order of complexity, maybe constant factor optimization
      - id: value
        size: _root.deserialization_helper.get_col_size.as<u8>
        if: (flags & 0x04) == 0 # only if does not have empty value

      - id: tmp_
        if: not complex
        size: _root.deserialization_helper.inc.as<u4>

    doc: |
      See https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L209
      for serialization info

  cell_path:
    seq:
      - id: length
        type: vint
      - id: value
        size: length.val.as<u4>

  complex_cell:
    seq:
      - id: complex_deletion_time
        type: delta_deletion_time
        if: (_parent._parent.flags & 0x40) != 0 # HAS_COMPLEX_DELETION set on row
      - id: items_count
        type: vint
      - id: simple_cells
        type: simple_cell(true) # "true" means "is a child of a complex cell"
        repeat: expr
        repeat-expr: items_count.val.as<u4>
      - id: tmp_
        size: _root.deserialization_helper.inc.as<u4>

  # ============================== RANGE TOMBSTONE MARKERS ==============================

  range_tombstone_marker:
    seq:
      - id: kind # stored as ordinal
        type: u1
        doc: |
          The kind of clustering prefix this is
          See https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L56
          Specifies the index of the kind in the following enum:
          ```java
          public enum Kind {
            EXCL_END_BOUND              (0, -1),
            INCL_START_BOUND            (0, -1),
            EXCL_END_INCL_START_BOUNDARY(0, -1),
            STATIC_CLUSTERING           (1, -1),
            CLUSTERING                  (2,  0),
            INCL_END_EXCL_START_BOUNDARY(3,  1),
            INCL_END_BOUND              (3,  1),
            EXCL_START_BOUND            (3,  1);
          }
          ```
      - id: bound_values_count
        type: u2
      - id: clustering_blocks
        type: clustering_blocks
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

      See https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L338

      See https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L220
      for serialization info