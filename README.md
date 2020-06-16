# Snowflake Extract Load Transorm framework  
Why in Snowflet L comes before E? I really like the sound of Snowflet

## env variable required
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




## *class* snowflet.db.DBExecutor() <br />
Snowflake API wrapper <br />

### Methods
**validate_connection()** return the snowflake version <br />
**query_exec()** execute the sql query  <br />
**Parameters:**
- **file_query**: path to the query file, either this or query shall be passed, can contain {parameters} 
- **query**: sql query to be executed, can contain {parameters}  
- **return_df**: Defaulted to False, passed True in case of SELECT query, it returns a pandas dataframe 
- ****kwargs**: parameters in the sql are replaced with the corrispective kwarg value
```
    """ example """
    newdb = db()
    newdb.query_exec(
            query="create database {db}",
            db=test     #  {db} is replaced by test in the sql query        
        ) # database test is created
```
### Usage
```
db = db() # initiate the snowflake connection using env variables
db.close() # close and dismiss the connection
```
## Pipeline executor