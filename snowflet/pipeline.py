import copy
import time
import random
import logging
import threading
import numpy as np
import concurrent.futures
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
from snowflet.lib import add_database_id_prefix
from snowflet.toolkit import read_yaml_file
import random


def execute_parallel(func, args, message='running task', log=''):
    """
    execute the functions in parallel for each list of parameters passed in args

    Arguments:
    func: function as an object
    args: list of function's args

    """
    tasks = []
    count = []

    # logging.basicConfig(format=format, level=logging.INFO,
    #                      datefmt="%H:%M:%S")
   
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_func = {executor.submit(func, **arg): arg for arg in args}
        for future in concurrent.futures.as_completed(future_to_func):
            arg = future_to_func[future]
            try:
                ret = future.result()
            except AssertionError as ass_exc:
                logging.info(f"{message} {arg.get(log,'')}: failed")
                raise AssertionError
            except Exception as exc:
                logging.info('%r generated an exception: %s' % (arg, exc))
                raise Exception            
            else:
                logging.info(f"{message} {arg.get(log,'')}") 

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
            add_database_id_prefix(obj=self.yaml, prefix=self.dry_run_dataset_prefix, kwargs=self.kwargs)

