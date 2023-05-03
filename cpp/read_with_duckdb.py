import duckdb
# to start an in-memory database
con = duckdb.connect(database=':memory:')
con.sql("SELECT count(*) FROM './out.parquet'").show()
#print(con.fetchall())

#con.execute("describe table './out.parquet'")
#print(con.fetchall())

con.sql("SELECT partition_key, _ts_row_liveness, _del_time_row_liveness, _local_del_time_partition, _marked_for_del_at_partition, _marked_for_delete_at_row, base_fee, _ts_base_fee, _del_time_base_fee, _ttl_base_fee FROM './out.parquet' limit 10").show()

con.execute("SELECT * FROM './out.parquet' limit 10")
print(con.fetchall())