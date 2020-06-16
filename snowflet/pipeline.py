from snowflet.db import DBExecutor as db
from snowflet.lib import logging_config
from snowflet.lib import default_user
from snowflet.lib import default_role
from snowflet.lib import default_schema
from snowflet.lib import default_account
from snowflet.lib import default_database
from snowflet.lib import default_password
from snowflet.lib import default_timezone
from snowflet.lib import default_warehouse
from snowflet.toolkit import read_yaml_file
import random

class PipelineExecutor:

    def __init__(
        self,
        yaml_file,
        account = default_account(),
        user = default_user(),
        password = default_password(),
        database = default_database(),
        schema = default_schema(),
        warehouse = default_warehouse(),
        role=default_role(),
        timezone = default_timezone(),
        dry_run=False, 
        *args, 
        **kwargs
    ):
        self.db = db(
            account=account, user=user, password=password, 
            database=database, schema=schema, warehouse=warehouse, 
            role=role, timezone=timezone
        )
     
        self.kwargs = kwargs
        self.yaml = read_yaml_file(yaml_file)
        self.dry_run_dataset_prefix = None
        if dry_run:
            self.dry_run_dataset_prefix = random.sample(range(1,1000000000),1)[0]
            add_database_id(obj=self.yaml, prefix=self.dry_run_dataset_prefix, kwargs=self.kwargs)