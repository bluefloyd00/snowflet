# Snowflake Extract Load Transorm framework  
Why in Snowflet L comes before E? I really like the sound of Snowflet

## env variable required
```
"PROJECT_ROOT": "${ProjectFolder}"             # REQUIRED
"ACCOUNT":  "gsXXXXX.west-europe.azure"        # REQUIRED
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

Parameters:
- **file_query**: path to the query file, either this or query shall be passed, can contain {parameters} 
- **query**: sql query to be executed, can contain {parameters}  
- **return_df**: Defaulted to False, passed True in case of SELECT query, it returns a pandas dataframe 
- **kwargs**: parameters in the sql are replaced with the corresponding kwarg value
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
## *class* snowflet.db.PipelineExecutor() <br />
Ad hoc tool for executing pipeline in snowflake, the tool read a yaml file which describe the pipeline steps, and provides method to either run the pipeline or test it (unit and/or uat) <br />

### Notes
All the query file shall be compliant with the follow (including CTE for mock data):
- database and schema shall be explicit i.e. "database"."schema"."table" or database.schema.table 



### Methods
**run()** execute the pipeline <br />

**clone_prod() TBD** clone the prod db metadata <br />

**clone_clean() TBD** removed the cloned databases <br />

### Usage
- for running the Pipeline

```
from snoflet import PipelineExecutor
pipeline = PipelineExecutor(
    "path_to_pipeline_folder/pipeline_name.yaml")     # initiate PipelineExecutor for Run
pipeline.run()                                        # run the pipeline
```

- for ci-cd (testing)

```
from snoflet import PipelineExecutor
pipeline = PipelineExecutor(
    "path_to_pipeline_folder/pipeline_name.yaml", 
    test=True
    )                                                 # initiate PipelineExecutor for testing
pipeline.run_unit_tests()                             # run all unit tests in parallel
try:
    pipeline.clone_prod()                    # copy tables' structure from prod
    pipeline.run()                                    # run the pipeline on empty tables (dry_run)
finally:
    pipeline.clone_clean()                          # cleans the dev/test environment
```

### YAML definition

**Structure:**

```
desc: 
databases: 
batches:    
release:
```

#### databases

list of database referenced in the pipeline
```
['database1', 'database2', 'database3']
```
#### release 
list of files that are executed before the execution of the pipeline

example
```
release:
  date: "2020-05-07"
  desc: "change table schema and delete a table from prod"
  files:
    - path_to_file1
```

#### batches

- contains the list of batches to execute
- the batches are execute in serial
- task within the batch runs in parallel

```
batches:
-   desc: creates table structure
    tasks:
-   desc: creates staging tables
    tasks:
-   desc: creates aggregated tables
    tasks:
```

**tasks:**
```
-   desc: creates aggregated tables
    tasks:
    -   desc: use Database
        object: query_executor
        args:
        -   file_query: path_to_file.sql
    -   desc: create table1
        object: create_table
        args:
        -   file: path_to_sql_query_select_file.sql
            table_schema: path_to_schema_definition_file.sql
            database_id: dbtest
            schema_id: sctest
            table_id: tbtest
            mock_file: path_to_mock_file.sql
            output_table_name: staging.attr_order_items_pk 
```
#### type of objects

- query_executor:

it is a wrapper of snowflet.db.exec_query, same parameters

- create_table:

it is a wrapper of snowflet.db.create_table, same parameters

