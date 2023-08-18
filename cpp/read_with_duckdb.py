import duckdb
# to start an in-memory database
con = duckdb.connect(database=':memory:')
con.sql("SELECT count(*) FROM './out.parquet'").show()
#print(con.fetchall())

con.sql("SELECT partition_key, count(*) as count FROM './out.parquet' group by partition_key order by count desc limit 10").show()

con.execute("describe table './out.parquet'")
print(con.fetchall())

con.execute("SELECT * FROM './out.parquet' limit 10")
print(con.fetchall())