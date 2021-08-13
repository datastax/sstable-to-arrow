import sstable_to_pyarrow as sstopy
print(sstopy.greet())
table = sstopy.create_table()
print(table.to_string())
