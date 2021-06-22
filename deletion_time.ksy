meta:
  id: deletion_time
  endian: be

seq:
  - id: local_deletion_time
    type: u4
    doc: |
      Server time in seconds when deletion occurred
      Compared to gc_grace_seconds to decide when it can be purged
      When used with time-to-live, encodes the time the data expires

  - id: marked_for_delete_at
    type: u8
    doc: |
      Timestamp of deletion
      Data with smaller timestamp considered deleted

doc: |
  See https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ColumnIndex.java#L98
