import duckdb

# connect to an in-memory temporary database
conn = duckdb.connect()

conn.execute("INSTALL sqlite; LOAD sqlite;")

conn.execute("ATTACH 'temp_airports.db' (TYPE SQLITE); USE temp_airports;")

# query the database as dataframe
check_duplicates = """
SELECT AirportID,
       COUNT(AirportID) AS occurrences
FROM airports
GROUP BY AirportID
HAVING COUNT(AirportID) > 1;
"""
df = conn.execute(check_duplicates).fetchdf()
assert df.shape[0] == 0, "Duplicate AirportID found in SQLite."

print(conn.execute("SELECT COUNT(1) FROM airports").fetchone()[0])
