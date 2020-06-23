from snowflet.db import DBExecutor as db

db = db() 
db.create_database(database_id="test_release")
db.create_schema(database_id="test_release", schema_id="test")
db.load_table(
    database_id="test_release",
    schema_id="test",
    table_id="test_release_table1",
    query="select 1 as col1",
    truncate=True              
)