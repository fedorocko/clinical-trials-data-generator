import duckdb

# Query CSV file directly
print(duckdb.sql("SELECT * FROM read_csv_auto('data/observations.csv')").fetchall()[0])