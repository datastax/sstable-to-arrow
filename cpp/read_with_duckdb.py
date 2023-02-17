import duckdb
# to start an in-memory database
con = duckdb.connect(database=':memory:')
con.execute("SELECT count(*) FROM './out.parquet'")
print(con.fetchall())

con.execute("describe table './out.parquet'")
print(con.fetchall())



#con.execute("SELECT * FROM './out.parquet'")
#con.execute("SELECT * FROM './out.parquet' limit 1")
#print(con.fetchall())
