import sstable_to_pyarrow as sstopy
print(sstopy.greet())
table = sstopy.create_table()
print(table.to_string())

table = sstopy.read_sstables("/workspaces/sstable-to-arrow/cpp/sample_data/baselines/iot-5b608090e03d11ebb4c1d335f841c590")

print('read table:')
print(table)
print(table[0].to_string())
print('first column:')
print(table[0].column(0))
