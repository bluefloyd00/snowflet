# Snowflake Extract Load Transorm framework  
Why in Snowflet L comes before E? I really like the sound of Snoflet

# env variable required
```
"PROJECT_ROOT": "${ProjectFolder}"             # REQUIRED
"ACCOUNT":  "i.e.gsXXXXX.west-europe.azure"    # REQUIRED
"USER": "user"                                 # REQUIRED
"PASSWORD": secret_password                    # REQUIRED
"DATABASE": "default_database"                 # OPTIONAL
"SCHEMA": "default_schema"                     # OPTIONAL
"WAREHOUSE": ""                                # OPTIONAL
"ROLE": ""                                     # OPTIONAL
"TIMEZONE": "europe/london"                    # OPTIONAL
```
# DB

Snowflake API wrapper


`class snowflet.db.DBExecutor()`
- validate_connection() - return the snowflake version
- query_exec() - execute the sql query
Parameters:
- file_query: path to the query file, either this or query shall be passed, can contain {parameters} 
- query: sql query to be executed, can contain {parameters}  
- return_df: Defaulted to False, passed True in case of SELECT query, it returns a pandas dataframe 
- **kwargs: these parameters are replaced with the sql
```
    example:
    newdb = db()
    newdb.query_exec(
            query="create database {db}",
            db=test     # test is replaced within the sql query        
        ) # database test is created
```
## usage
```
db = db() # initiate the snowflake connection using env variables
db.close() # close and dismiss the connection
```
# Pipeline executor