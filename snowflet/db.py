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
    
    def validate_connection(self):
        engine = create_engine(
            'snowflake://{user}:{password}@{account}/'.format(
                user=self.user,
                password=self.password,
                account=self.account
            )
        )
        results = None
        try:
            connection = engine.connect()
            results = connection.execute('select current_version()').fetchone()
            print(results[0])
            return results
        finally:
            connection.close()
            engine.dispose()