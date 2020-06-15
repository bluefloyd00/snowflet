from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine   
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
        account = default_account(),
        user = default_user(),
        password = default_password(),
        database = default_database(),
        schema = default_schema(),
        warehouse = default_warehouse(),
        role=default_role(),
        timezone = default_timezone()
        ):
        self.password=password
        self.user=user
        self.account=account
        self.database=database
        self.schema=schema
        self.warehouse=warehouse
        self.role=role
        self.timezone=timezone
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

    def check_connection_args(self, database, schema ):
        if database != self.database or schema!= self.schema:
            self.database=database
            self.schema=schema
            self.connect()
    
    def run_query(self,  file_query="", query="", database=default_database(), schema=default_schema()):
        self.check_connection_args(database, schema)
        if file_query != "":

            