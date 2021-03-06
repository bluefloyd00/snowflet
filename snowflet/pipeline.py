import copy
import time
import random
import logging
import threading
import numpy as np
import concurrent.futures
from snowflet.lib import apply_kwargs
from snowflet.lib import default_user
from snowflet.lib import default_role
from snowflet.lib import extract_args
from snowflet.lib import default_schema
from snowflet.lib import logging_config
from snowflet.lib import default_account
from snowflet.db import DBExecutor as db
from snowflet.lib import default_database
from snowflet.lib import default_password
from snowflet.lib import default_timezone
from snowflet.lib import default_warehouse
from snowflet.toolkit import read_yaml_file
from snowflet.lib import add_database_id_prefix





def execute_parallel(func_list, workers=10):
    """
    execute the functions in parallel for each list of parameters passed in args

    Arguments:
    func: function as an object
    args: list of function's args

    """

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_func = {executor.submit(f.get('object',''), **f.get('args','')): f for f in func_list}
        for future in concurrent.futures.as_completed(future_to_func):
            arg = future_to_func[future]
            try:
                res = future.result()
                logging.info(f"PipelineExecutor - {arg.get('desc','running task')}")
            except AssertionError as ass_exc:
                logging.error(ass_exc)
                logging.info(f"PipelineExecutor - {arg.get('desc','')}: failed")
                raise AssertionError
            except Exception as exc:
                logging.info('PipelineExecutor - %r generated an exception: %s' % (arg, exc))
                raise Exception

class PipelineExecutor:

    def __init__(
        self,
        yaml_file,
        account=default_account(),
        user=default_user(),
        password=default_password(),
        database_id=default_database(),
        schema_id=default_schema(),
        warehouse=default_warehouse(),
        role=default_role(),
        timezone=default_timezone(),
        dry_run=False,
        workers=10,
        *args,
        **kwargs
    ):
        self.db = db(
            account=account, user=user, password=password, 
            database_id=database_id, schema_id=schema_id, warehouse=warehouse, 
            role=role, timezone=timezone
        )
        self.workers = workers
        self.kwargs = kwargs
        self.yaml = read_yaml_file(yaml_file)
        self.clone_database_prefix = None
        if dry_run:
            self.clone_database_prefix = "CLONE_" + str(random.sample(range(1, 1000000000), 1)[0])
            add_database_id_prefix(
                obj=self.yaml,
                prefix=self.clone_database_prefix,
                kwargs=self.kwargs)
        

    def select_object(self, obj_name):
        # if obj_name == 'query_executor':
        #     return self.db.query_exec
        if obj_name == 'initiate_database_schema':
            return self.db.initiate_database_schema
        if obj_name == 'dop_database':
            return self.db.delete_database
        if obj_name == 'load_table':
            return self.db.load_table
        if obj_name == 'create_database':
            return self.db.create_database
        if obj_name == 'create_schema':
            return self.db.create_schema
        else:
            raise Exception("No matching object")

    def map_objects(self, tasks):
        for task in tasks:
            for key_, value_ in task.items():
                if key_ == 'object':
                    task.update({key_: self.select_object(value_)})
            
            task['args'].update(self.kwargs)
            task['args'].update({'clone_database_prefix' : self.clone_database_prefix})

    def run_batch(self, batch):
        tasks = batch.get('tasks', '')
        self.map_objects(tasks)
        if tasks == []:
            raise Exception("PipelineExecutor - load_google_sheet in yaml is not well defined")
        execute_parallel(
                    tasks,
                    workers=self.workers
                )


    def run(self):
        # run release (ToDo)
        # run batches
        batch_list = self.yaml.get('batches', '')
        for batch in batch_list:
            apply_kwargs(batch, self.kwargs)  ## resolve environment variable passed as kwargs
            self.run_batch(batch)
    
    def clone_clean(self):
        dabatase_list = self.yaml.get('databases', '')
        for database in dabatase_list:
            clone_database = str(self.clone_database_prefix) + "_" + database
            if "CLONE_" in clone_database:
                self.db.delete_database(clone_database)

    def clone_prod(self, with_data=True):
        dabatase_list = self.yaml.get('databases', '')
        for database in dabatase_list:
            clone_database = str(self.clone_database_prefix) + "_" + database
            self.db.clone_database(
                database_id=database,
                clone_prefix=self.clone_database_prefix
            )
            tables=self.db.list_tables(database_id=clone_database)
            if not with_data:
                for table in tables:
                    self.db.query_exec(
                        query=""" DELETE FROM {table} """, 
                        table=table
                    )