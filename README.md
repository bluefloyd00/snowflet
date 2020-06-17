# Snowflake Extract Load Transorm framework  
Why in Snowflet L comes before E? I really like the sound of Snowflet

## env variable required
```
"PROJECT_ROOT": "${ProjectFolder}"             # REQUIRED
"ACCOUNT":  "gsXXXXX.west-europe.azure"    # REQUIRED
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
    dry_run=True
    )                                                 # initiate PipelineExecutor for testing
pipeline.run_unit_tests()                             # run all unit tests in parallel
try:
    pipeline.clone_prod_metadata()                    # copy tables' structure from prod
    pipeline.run()                                    # run the pipeline on empty tables (dry_run)
finally:
    pipeline.dry_run_clean()                          # cleans the dev/test environment
```