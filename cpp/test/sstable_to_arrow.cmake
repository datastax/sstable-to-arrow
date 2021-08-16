enable_testing()

add_test(NAME Runs COMMAND sstable_to_arrow_exe)

add_test(NAME Usage COMMAND sstable_to_arrow_exe -h)
set_tests_properties(
  Usage
  PROPERTIES
    PASS_REGULAR_EXPRESSION
    "========================= sstable-to-arrow =========================")

add_test(NAME SampleData COMMAND sstable_to_arrow_exe -sd)
set_tests_properties(
  SampleData
  PROPERTIES
    PASS_REGULAR_EXPRESSION
    "195EDDA7038B417C99C98F001C637E68"
    "28DF63B7CC5743CB9752FAE69D1653DA"
    "Nullam sollicitudin ullamcorper turpis, mollis hendrerit dui. Integer posuere, purus eu sodales fringilla, velit n"
)

add_test(NAME SampleDataForCudf COMMAND sstable_to_arrow_exe -sdx)
set_tests_properties(SampleDataForCudf PROPERTIES PASS_REGULAR_EXPRESSION
                                                  "partition_key_part2:")

add_test(NAME NoMetadata COMMAND sstable_to_arrow_exe -scd)
set_tests_properties(NoMetadata PROPERTIES FAIL_REGULAR_EXPRESSION "_ttl_"
                                           "_del_time_" "_ts_")

add_test(
  NAME ReadFromS3Uri
  COMMAND
    sstable_to_arrow_exe -dx
    s3://sstable-to-arrow/iot-1k/baselines/iot-5b608090e03d11ebb4c1d335f841c590/
)
set_tests_properties(
  ReadFromS3Uri
  PROPERTIES
    PASS_REGULAR_EXPRESSION
    "opening connection to s3"
    "closing connection to s3"
    "partition_key_part1: struct<org.apache.cassandra.db.marshal.UUIDType: uint64, org.apache.cassandra.db.marshal.UTF8Type: string>"
)
