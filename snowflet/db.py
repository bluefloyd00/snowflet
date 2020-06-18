import pandas as pd
import logging
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


class DBExecutor:
    def __init__(
        self,
        account=default_account(),
        user=default_user(),
        password=default_password(),
        database=default_database(),
        schema=default_schema(),
        warehouse=default_warehouse(),
        role=default_role(),
        timezone=default_timezone()
    ):
        self.password = password
        self.user = user
        self.account = account
        self.database = database
        self.schema = schema
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

    def check_connection_args(self, database, schema):
        if database != self.database or schema != self.schema:
            self.database = database
            self.schema = schema
            self.connect()
    
    def create_database(self, database_id):
        if self.database_exists(database_id):
            logging.info(
                "Dataset %s already exists",
                database_id
            )
        else:
            try:
                self.query_exec(
                    query="CREATE DATABASE {db}",
                    db=database_id
                )
                logging.info(
                    "Created database: %s",
                    database_id
                )
            except Exception as error:
                logging.error(error)
        
    def use_database(self, database_id):
        self.query_exec(
                query="USE DATABASE  {db}",
                db=database_id
            )

    def delete_database(self, database_id):
        if not self.database_exists(database_id):
            logging.info(
                "Dataset %s does not exists",
                database_id
            )
        else:
            try:
                
                self.query_exec(
                        query="DROP DATABASE {db}",
                        db=database_id
                    )
                logging.info(
                    "Dropped database: %s",
                    database_id
                )
            except Exception as error:
                logging.error(error)

    def create_schema(self, schema_id, database_id=default_database):
        self.query_exec(
                query="CREATE SCHEMA {db}.{schema}",
                db=database_id,
                schema=schema_id
            )

    def database_exists(self, database_id):

        result = self.query_exec(
                    query="SHOW DATABASES",
                    return_df=True
                )

        if database_id.upper() in result['name'].values.tolist():
            return True
        else:
            return False

    def table_exists(self, database_id, schema_id, table_id):

        result = self.query_exec(
                    query="SHOW TABLES IN  {db}.{schema} ",
                    return_df=True,
                    db=database_id.upper(),
                    schema=schema_id.upper()
                )

        if table_id.upper() in result['name'].values.tolist():
            return True
        else:
            return False

    def initiate_database_schema(self, database_id, schema_id):
        self.create_database(database_id=database_id)
        self.create_schema(schema_id=schema_id,database_id=database_id)
        

    def query_exec(self,  file_query="", query="", return_df=False, *args, **kwargs):
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
