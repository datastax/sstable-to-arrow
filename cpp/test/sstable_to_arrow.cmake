enable_testing()

add_test(NAME Runs COMMAND sstable_to_arrow)

add_test(NAME Usage COMMAND sstable_to_arrow -h)
set_tests_properties(
  Usage
  PROPERTIES
    PASS_REGULAR_EXPRESSION
    "========================= sstable-to-arrow =========================")

add_test(NAME SampleData COMMAND sstable_to_arrow -sd)
set_tests_properties(
  SampleData
  PROPERTIES
    PASS_REGULAR_EXPRESSION
    "195EDDA7038B417C99C98F001C637E68"
    "28DF63B7CC5743CB9752FAE69D1653DA"
    "Nullam sollicitudin ullamcorper turpis, mollis hendrerit dui. Integer posuere, purus eu sodales fringilla, velit n"
)

add_test(NAME NoMetadata COMMAND sstable_to_arrow -scd)
set_tests_properties(NoMetadata PROPERTIES FAIL_REGULAR_EXPRESSION "_ttl_"
                                           "_del_time_" "_ts_")
