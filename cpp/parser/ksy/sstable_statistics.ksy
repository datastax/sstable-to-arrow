# see https://docs.scylladb.com/architecture/sstable/sstable3/sstables_3_statistics/

meta:
  id: sstable_statistics
  endian: be
  ks-opaque-types: true

seq:
  - id: toc
    type: toc

types:
  toc:
    seq:
      - id: length
        type: s4
      - id: idk
        type: s4
      - id: array
        type: toc_entry
        repeat: expr
        repeat-expr: length
    doc: |
      Contains metadata about the table.
      0: validation_metadata
      1: compaction_metadata
      2: statistics
      3: serialization_header

  toc_entry:
    seq:
      - id: type
        type: s4
      - id: offset
        type: s4
    instances:
      body:
        pos: offset
        type:
          switch-on: type
          cases:
            0: validation_metadata
            1: compaction_metadata
            2: statistics
            3: serialization_header

  validation_metadata:
    seq: # modified UTF-8 format, might need external opaque type
      - id: partitioner_name
        type: modified_utf8
#        type: str
#        size: 45
#        encoding: utf8
      - id: bloom_filter_fp_chance
        type: f8

  statistics:
    seq:
      - id: partition_sizes
        type: estimated_histogram
      - id: column_counts
        type: estimated_histogram
      - id: commit_log_upper_bound
        type: commit_log_position
      - id: min_timestamp
        type: s8
      - id: max_timestamp
        type: s8
      - id: min_local_deletion_time
        type: s4
      - id: max_local_deletion_time
        type: s4
      - id: min_ttl
        type: s4
      - id: max_ttl
        type: s4
      - id: compression_rate
        type: f8
      - id: tombstones
        type: streaming_histogram
      - id: level
        type: s4
      - id: repaired_at
        type: s8
      - id: clustering_types
        type: clustering_type_list
      - id: covered_clustering_start_kind
        type: s1
      - id: covered_clustering_start_length
        type: s2
      - id: covered_clustering_start
        type:
          switch-on: covered_clustering_start_kind
          cases:
          # TODO support other cases
            1: incl_start_bound
            6: incl_end_bound
      # TODO: support the case where the lengths are different for start and end bounds
      # currently this end length gets ignored
      - id: covered_clustering_end_kind
        type: s1
      - id: covered_clustering_end_length
        type: s2
      - id: covered_clustering_end
        type:
          switch-on: covered_clustering_end_kind
          cases:
          # TODO support other cases
            1: incl_start_bound
            6: incl_end_bound
      - id: has_legacy_counter_shards
        type: b1
        # TODO: actually pull data in this scenario
#      - id: legacy_counter_shards
#        type: u1
#        if: has_legacy_counter_shards
      - id: number_of_columns
        type: s8
      - id: number_of_rows
        type: s8

#      - id: min_clustering_key
#        type: clustering_bound
#      - id: max_clustering_key
#        type: clustering_bound
#      - id: has_legacy_counters
#        type: s1 # bool


      # version MA of SSTable 3.x ends here

      - id: commit_log_lower_bound
        type: commit_log_position

      # version MB of SSTable 3.x ends here

      - id: commit_log_intervals
        type: commit_log_intervals
        
        # TODO: some new MISSING STUFF
        # pending repair
        # zero copy metadata
        # incremental node sync info
        # [max] column value lengths
        # partition level deletes
        # originating host id

  incl_start_bound:
    seq:
      - id: header # used for a null check? don't think it ever happens from valid sstables
        #type: u1 # vint
        type: vint
# This logic is based on the data type which is pretty hairy
# https://github.com/riptano/bdp/blob/6.8-dev/dse-db/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L480
      - id: value
        repeat: expr
        repeat-expr: _parent.covered_clustering_start_length
#        if:  ( _parent.clustering_types.entry[4].type == _parent.clustering_types.entry[0].type )
#        if: _parent.clustering_types.entry[_index] = 'apache.cassandra.db.marshal.UTF8Type'
#        if: false
        type:
          switch-on: _parent.clustering_types.entry[_index].type
          cases:
# TODO:  add other types
            '"org.apache.cassandra.db.marshal.Int32Type"': int32type
            '"org.apache.cassandra.db.marshal.LongType"': longtype
            '"org.apache.cassandra.db.marshal.UTF8Type"': utf8type
            '"org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type)"': utf8type
            '"org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.LongType)"': longtype
  incl_end_bound:
    seq:
      - id: header # used for a null check? don't think it ever happens from valid sstables
        #type: u1 # vint
        type: vint
# TODO: this thing actually gets bounds per column type
# this means the logic is based on the data type which is pretty hairy
# https://github.com/riptano/bdp/blob/6.8-dev/dse-db/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L480
      - id: value
        repeat: expr
        repeat-expr: _parent.covered_clustering_end_length
#        if:  ( _parent.clustering_types.entry[4].type == _parent.clustering_types.entry[0].type )
#        if: _parent.clustering_types.entry[_index] = 'apache.cassandra.db.marshal.UTF8Type'
#        if: false
        type:
          switch-on: _parent.clustering_types.entry[_index].type
          cases:
# TODO:  add other types
            '"org.apache.cassandra.db.marshal.Int32Type"': int32type
            '"org.apache.cassandra.db.marshal.LongType"': longtype
            '"org.apache.cassandra.db.marshal.UTF8Type"': utf8type
            '"org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type)"': utf8type
            '"org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.LongType)"': longtype

            
  int32type:
    seq:
      - id: value
        type: s4
            
  longtype:
    seq:
      - id: value
        type: s8

  utf8type:
    seq:
      - id: length
        #type: u1 # vint
        type: vint
      - id: value
        type: str
        encoding: utf-8
        size: length.val.as<u1>

  compaction_metadata:
    seq:
      - id: length
        type: s4
      - id: array
        size: 1
        repeat: expr
        repeat-expr: length
    doc: |
      Serialized HyperLogLogPlus which can be used to estimate the number of partition keys in the SSTable.
      If this is not present then the same estimation can be computed using Summary file.
      Encoding is described in:
      https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/cardinality/HyperLogLogPlus.java


  estimated_histogram:
    seq:
      - id: length
        type: bucket_array

  streaming_histogram:
    seq:
      - id: bucket_number_limit
        type: s4
      - id: buckets
        type: bucket_array

  bucket_array:
    seq:
      - id: length
        type: s4
      - id: array
        type: bucket
        repeat: expr
        repeat-expr: length

  bucket:
    seq:
      - id: prev_bucket_offset
        type: s8
      - id: value
        type: s8

  clustering_type_list:
    seq:
      - id: size
        type: u1
      - id: entry
        type: clustering_type_list_entry
        repeat: expr
        repeat-expr: size

  clustering_type_list_entry:
    seq:
      - id: length
        type: u1
      - id: type
        type: str
        encoding: UTF-8
        size: length


  commit_log_position:
    seq:
      - id: segment_id
        type: s8
      - id: position_in_segment
        type: s4

  clustering_bound:
    seq:
      - id: length
        type: s4
      - id: array
        type: clustering_column
        repeat: expr
        repeat-expr: length

  clustering_column:
    seq:
      - id: length
        type: u2
      - id: array
        size: 1
        repeat: expr
        repeat-expr: length

  commit_log_interval:
    seq:
      - id: start
        type: commit_log_position
      - id: end
        type: commit_log_position

  commit_log_intervals:
    seq:
      - id: length
        type: s4
      - id: array
        type: commit_log_interval
        repeat: expr
        repeat-expr: length

  # ==========

  serialization_header:
    seq:
      - id: min_timestamp
        #type: u8 # vint # u8
        type: vint # u8
      - id: min_local_deletion_time
        #type: u4 # vint # u4
        type: vint # u4
      - id: min_ttl
        #type:  u4 # vint # u4
        type:  vint # u4
      - id: partition_key_type
        type: string_type
      - id: clustering_key_types
        type: clustering_key_types
      - id: static_columns
        type: columns
      - id: regular_columns
        type: columns

  clustering_key_types:
    seq:
      - id: length
        #type: u4 # u4 # vint
        type: vint
      - id: array
        type: string_type
        repeat: expr
        #repeat-expr: length #.val.as<u4>
        repeat-expr: length.val.as<u4>

  columns:
    seq:
      - id: length
        #type: u4 # vint
        type: vint
      - id: array
        type: column
        repeat: expr
        #repeat-expr: length #.val.as<u4>
        repeat-expr: length.val.as<u4>

  column:
    seq:
      - id: name
        type: string_type
      - id: column_type
        type: string_type

  string_type:
    seq:
      - id: length
        #type: u4 # vint
        type: vint
      - id: body
        #size: length #.val.as<u4>
        size: length.val.as<u4>
        type: str
        encoding: UTF-8