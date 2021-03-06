import logging
import pandas as pd
# from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine   
from snowflet.lib import read_sql
from snowflet.lib import default_user
from snowflet.lib import default_role
from snowflet.lib import default_schema
from snowflet.lib import default_account
from snowflet.lib import default_database
from snowflet.lib import default_password
from snowflet.lib import default_timezone
from snowflet.lib import default_warehouse
from snowflet.lib import uppercase_parameters



class DBExecutor:
    def __init__(
        self,
        account=default_account(),
        user=default_user(),
        password=default_password(),
        database_id=default_database(),
        schema_id=default_schema(),
        warehouse=default_warehouse(),
        role=default_role(),
        timezone=default_timezone()
    ):
        self.password = password
        self.user = user
        self.account = account
        self.database = database_id
        self.schema = schema_id
        self.warehouse = warehouse
        self.role = role
        self.timezone = timezone
        self.connect()

    def connect(self):
        self.engine = create_engine(
            'snowflake://{user}:{password}@{account}/'.format(
                user=self.user,
                password=self.password,
                account=self.account
            )
        )

        self.connection = self.engine.connect()

    def close(self):
        self.connection.close()
        self.engine.dispose()

    def validate_connection(self):
        results = None
        results = self.connection.execute('select current_version()').fetchone()
        return results

    @uppercase_parameters
    def check_connection_args(self, database, schema, *args, **kwargs):
        if database != self.database or schema != self.schema:
            self.database = database
            self.schema = schema
            self.connect()
    
    @uppercase_parameters
    def clone_database(self, database_id, clone_prefix, *args, **kwargs):
        if not self.database_exists(database_id):
            logging.info(
                "Database %s does not exists",
                database_id
            )
        else:
            try:
                clone_database = clone_prefix + "_" + database_id
                self.query_exec(
                    query= """ CREATE DATABASE {clone} CLONE {db} """,
                    clone=clone_database,
                    db=database_id,                
                    **kwargs
                )
                logging.info(
                    "Cloned database: %s",
                    database_id
                )
            except Exception as error:
                logging.error(error)

    @uppercase_parameters
    def create_database(self, database_id, *args, **kwargs):
        if self.database_exists(database_id):
            logging.info(
                "Database %s already exists",
                database_id
            )
        else:
            try:
                self.query_exec(
                    query="CREATE DATABASE {db}",
                    db=database_id,
                    **kwargs
                )
                logging.info(
                    "Created database: %s",
                    database_id
                )
            except Exception as error:
                logging.error(error)
    
    @uppercase_parameters
    def use_database(self, database_id, *args, **kwargs):
        self.query_exec(
                query="USE DATABASE  {db}",
                db=database_id,
                **kwargs
            )

    @uppercase_parameters
    def delete_database(self, database_id, *args, **kwargs):
        if not self.database_exists(database_id):
            logging.info(
                "Database %s does not exists",
                database_id
            )
        else:
            try:
                
                self.query_exec(
                        query="DROP DATABASE {db}",
                        db=database_id,
                        **kwargs
                    )
                logging.info(
                    "Dropped database: %s",
                    database_id
                )
            except Exception as error:
                logging.error(error)

    @uppercase_parameters
    def create_schema(self, schema_id, database_id=default_database(), *args, **kwargs):
        if self.schema_exists(database_id=database_id, schema_id=schema_id):
            logging.info(
                "Schema %s.%s already exists",
                database_id,
                schema_id
            )
        else:
            try:
                self.query_exec(
                    query="CREATE SCHEMA {db}.{schema}",
                    db=database_id,
                    schema=schema_id,
                    **kwargs
                )
                logging.info(
                    "Created schema: %s.%s",
                    database_id,
                    schema_id
                )
            except Exception as error:
                logging.error(error)

    @uppercase_parameters
    def delete_schema(self, schema_id, database_id=default_database(), *args, **kwargs):
        if not self.schema_exists(database_id=database_id, schema_id=schema_id):
            logging.info(
                "Schema %s.%s does not exists",
                database_id,
                schema_id
            )
        else:
            try:
                self.query_exec(
                    query="DROP SCHEMA {db}.{schema}",
                    db=database_id,
                    schema=schema_id,
                    **kwargs
                )
                logging.info(
                    "Dropped schema: %s.%s",
                    database_id,
                    schema_id
                )
            except Exception as error:
                logging.error(error)
        
    @uppercase_parameters
    def database_exists(self, database_id, *args, **kwargs):

        result = self.query_exec(
                    query="SHOW DATABASES",
                    return_df=True,
                    **kwargs
                )

        if database_id.upper() in result['name'].values.tolist():
            return True
        else:
            return False

    @uppercase_parameters
    def schema_exists(self, schema_id, database_id, *args, **kwargs):
        result = self.query_exec(
                    query="SHOW SCHEMAS IN DATABASE {db}",
                    return_df=True,
                    db=database_id,
                    **kwargs
                )

        if schema_id.upper() in result['name'].values.tolist():
            return True
        else:
            return False

    @uppercase_parameters
    def table_exists(self, database_id, schema_id, table_id, *args, **kwargs):

        result = self.query_exec(
                    query="SHOW TABLES IN  {db}.{schema}",
                    return_df=True,
                    db=database_id.upper(),
                    schema=schema_id.upper(),
                    **kwargs
                )

        if table_id.upper() in result['name'].values.tolist():
            return True
        else:
            return False
    
    @uppercase_parameters
    def list_tables(self, database_id, schema_id="", *args, **kwargs):
        if not self.database_exists(database_id=database_id):
            return []
        if schema_id=="":
            sql = """   select TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS name
                        from {db}.information_schema.tables
                        where table_schema != 'INFORMATION_SCHEMA'    """
        else: 
            sql = """   select TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS name
                        from {db}.information_schema.tables
                        where table_schema = '{schema}' """  
                       
        result = self.query_exec(
                    query=sql,
                    return_df=True,
                    db=database_id.upper(),
                    schema=schema_id.upper(),
                    **kwargs
                )

        return result['name'].values.tolist()

    @uppercase_parameters
    def initiate_database_schema(self, database_id, schema_id, *args, **kwargs):
        self.create_database(database_id=database_id, **kwargs)
        self.create_schema(schema_id=schema_id,database_id=database_id, **kwargs)

    @uppercase_parameters
    def query_exec(self,  file_query="", query="", return_df=False, *args, **kwargs):
        try:
            self.validate_connection()
        except:
            self.connect()
        result = None
        sql = read_sql(file_query, query, **kwargs)
        try:
            if return_df:
                result = pd.read_sql_query(sql, self.engine)
            else:
                result = self.connection.execute(sql)
        except Exception:
            self.close()
            raise Exception
        return result

    # def is_schema_ddl_equal(
    #     self,
    #     database_id,
    #     schema_id,
    #     table_id,
    #     ddl_file,
    #     **kwargs
    # ):
    #     ddl = read_sql( file=ddl_file, **kwargs)
    #     ddl_returned = self.query_exec(
    #         query=""" SELCT  GET_DDL( 'table' , '{}.{}.{}' ) """.format( database_id, schema_id, table_id )
    #     )
    #     assert string.join(ddl_returned.fetchall()) == ddl

    @uppercase_parameters
    def load_table(
        self, 
        database_id, 
        schema_id, 
        table_id, 
        ddl_file=None, 
        file_query="", 
        query="",
        df="",
        truncate=False, 
        *args, 
        **kwargs
        ):

        if [file_query, query, df].count("") != 2:
            raise Exception("One between file_query, query and df shall be provided")

        sql_part_1, sql_part_2 = "", ""

        if not self.table_exists(
            database_id=database_id,
            schema_id=schema_id,
            table_id=table_id
        ):
            # table does not exist
            if ddl_file is not None:
                self.query_exec(
                    query=read_sql(file=ddl_file),
                    **kwargs
                ) # create the table
            else:
                sql_part_1 = """ CREATE TABLE "{}"."{}"."{}" AS """.format(database_id, schema_id, table_id) # define first part
        else:
            logging.info("TBD: assert on ddl table vs ddl file")
            # self.is_table_schema_vs_ddl(table_id, ddl_file)  # IF THE TABLE SCHEMA IS DIFFERENT FROM THE DDL RAISE AN ERROR
            overwrite = ""
            if truncate:
                overwrite = " OVERWRITE "        
            sql_part_1 = """ INSERT {} INTO {}.{}.{}  """.format(overwrite, database_id, schema_id, table_id)

        
        if df != "":
            df.to_sql(
                '{}.{}.{}'.format( database_id, schema_id, table_id), 
                con=self.engine, 
                index=False
            )
        else:
            sql_part_2 = read_sql(file_query, query)
            sql = sql_part_1 + sql_part_2
            self.query_exec(query=sql, **kwargs)

        logging.info(
            'Query results loaded to table %s.%s.%s',
                database_id,
                schema_id,
                table_id
            )


