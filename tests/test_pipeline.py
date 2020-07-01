import time
import logging
import unittest
# from unittest import mock
import pandas as pd
import snowflet.pipeline as pl
from snowflet.lib import logging_config
from snowflet.lib import df_assert_equal
from snowflet.lib import add_database_id_prefix



class TestCloneDatabases(unittest.TestCase):

    def setUp(self):
        self.db = pl.db()
        self.db.initiate_database_schema(database_id="DB1", schema_id='TEST1')
        self.db.load_table(
                database_id="DB1",
                schema_id="TEST1",
                table_id="TABLE1",
                query="select 1 as col1",
                truncate=True              
            )
        self.db.initiate_database_schema(database_id="DB2", schema_id='TEST2')
        self.db.load_table(
                database_id="DB2",
                schema_id="TEST2",
                table_id="TABLE2",
                query="select 2 as col2",
                truncate=True              
            )

        self.pipeline = pl.PipelineExecutor(
            yaml_file="tests/yaml/clone.yaml",
        )
        self.pipeline.clone_database_prefix = "CLONE_1001"

    def tearDown(self):
        self.db = pl.db()
        self.db.delete_database(database_id="DB1")
        self.db.delete_database(database_id="DB2")
        self.db.delete_database(database_id="CLONE_1001_DB1")
        self.db.delete_database(database_id="CLONE_1001_DB2")
        
    def test_clone_with_data(self):
        self.pipeline.clone_prod(with_data=True)
        self.assertTrue(
            self.pipeline.db.table_exists(database_id="CLONE_1001_DB1", schema_id="TEST1", table_id="TABLE1"),
            "clone_without_data pipeline not properly run"
            )

    def test_clone_without_data(self):
        self.pipeline.clone_prod(with_data=False)
        result_df = self.pipeline.db.query_exec(
            query=""" SELECT * FROM  CLONE_1001_DB2.TEST2.TABLE2 """,
            return_df=True
        )
             
        self.assertTrue(result_df.dropna().empty)

class TestDryRun(unittest.TestCase):
    def setUp(self):
        self.pipeline = pl.PipelineExecutor(
            yaml_file="tests/yaml/dry_run.yaml"
        )
        self.pipeline.db.initiate_database_schema(
            database_id="test_db_dry_run1",
            schema_id="test_schema_dry_run1"
        )
        self.pipeline.db.load_table(
            database_id="test_db_dry_run1",
            schema_id="test_schema_dry_run1", 
            table_id="TABLE1",
            truncate=True,
            query=""" SELECT 1 AS COL1, 'A' as COL2 """
        )
        self.pipeline.clone_database_prefix="CLONE_1001"
        add_database_id_prefix(
                obj=self.pipeline.yaml,
                prefix=self.pipeline.clone_database_prefix,
                kwargs=self.pipeline.kwargs)
        

    def test_uat_run(self):
        self.pipeline.clone_prod(with_data=True)
        self.pipeline.run()
        returned_df = self.pipeline.db.query_exec(
                            query="select * from CLONE_1001_test_db_dry_run2.test_schema_dry_run2.table3 ",
                            return_df=True
                        )
        df_assert_equal(
            returned_df,   
            pd.DataFrame(data=[[1,'A','B']], columns=['col1','col2','col3'])
        )

    def test_dry_run_no_data(self):
        self.pipeline.clone_prod(with_data=False)
        self.pipeline.run()
        returned_df = self.pipeline.db.query_exec(
                            query="select * from CLONE_1001_test_db_dry_run2.test_schema_dry_run2.table3 ",
                            return_df=True
                        )
        print(returned_df)
        self.assertTrue(
            returned_df.empty,   
            "problems with test_dry_run"
            )
    
    def tearDown(self):
        """ Test """
        self.pipeline.db.delete_database(database_id="test_db_dry_run1")
        self.pipeline.db.delete_database(database_id="test_db_dry_run2")
        self.pipeline.clone_clean()
        self.pipeline.db.close()

class TestRun(unittest.TestCase):
    def setUp(self):
        self.pipeline = pl.PipelineExecutor("tests/yaml/run_batches.yaml")

    def test_run_initiate_db_execute_query(self):
        self.pipeline.run()
        self.assertTrue(
            self.pipeline.db.table_exists(database_id="TEST_RUN_BATCH", schema_id="TEST_SCHEMA", table_id="table1"),
            "test_run_batch pipeline not properly run"
            )
    
    def tearDown(self):
        """ Test """
        self.pipeline.db.delete_database(database_id="TEST_RUN_BATCH")
        self.pipeline.db.close()

class TestTask(unittest.TestCase):
    def setUp(self):
        self.pipeline = pl.PipelineExecutor("tests/yaml/task.yaml")

    def test_map_object(self):
        tasks = [   
            {
                "desc": "running first task",
                "object": 'load_table',
                "args": {'clone_database_prefix': None, 'num': 1}
            },
            {
                "desc": "running second task",
                "object": 'load_table',
                "args": {'clone_database_prefix': None, "num": 2}
            }
        ]
        expected_result = [   
            {
                "desc": "running first task",
                "object": self.pipeline.db.load_table,
                "args": {'clone_database_prefix': None, "num": 1}
            },
            {
                "desc": "running second task",
                "object": self.pipeline.db.load_table,
                "args": {'clone_database_prefix': None, "num": 2}
            }
        ]
        self.pipeline.map_objects(tasks)
        self.assertEqual( 
            tasks,
            expected_result,
            "Failed to map the objects"
        )
        
class TestExecute_parallel(unittest.TestCase):

    def sleep_2_sec(self, num):
        time.sleep(2)
        f = num + 2
        return f
    
    def test_func_exec(self):
        start_time = time.time()
        func_list = [   
            {
                "desc": "running first task",
                "object": self.sleep_2_sec,
                "args": {"num": 1}
            },
            {
                "desc": "running second task",
                "object": self.sleep_2_sec,
                "args": {"num": 2}
            },
            {
                "desc": "running third task",
                "object": self.sleep_2_sec,
                "args": {"num": 3}
            },
            {
                "object": self.sleep_2_sec,
                "args": {"num": 4}
            },
            {
                "object": self.sleep_2_sec,
                "args": {"num": 5}
            },
            {
                "object": self.sleep_2_sec,
                "args": {"num": 6}
            },
            {
                "object": self.sleep_2_sec,
                "args": {"num": 7}
            },
            {
                "desc": "running last task",
                "object": self.sleep_2_sec,
                "args": {"num": 8}
            }
        ]

        pl.execute_parallel(
                    func_list
                )

        end_time = time.time()
        self.assertLess(end_time-start_time, 3, "right execution time")

   

    def bug(self, **kwargs):
        for key, value in kwargs.items():
            if value == "error":
                raise Exception("error something")
            else:
                return 1

    def test_error_raised(self):
        with self.assertRaises(Exception):
            pl.execute_parallel(                    
                    [   
                        {
                            "desc": "running first task",
                            "object": self.bug,
                            "args": {"status": "ok"}
                        },
                        {
                            "desc": "running second task",
                            "object": self.bug,
                            "args": {"status": "ok"}
                        },
                        {
                            "desc": "running bug task",
                            "object": self.bug,
                            "args": {"status": "error"}
                        },
                        {
                            "desc": "running first task",
                            "object": self.bug,
                            "args": {"status": "ok"}
                        }
                    ]
                )


if __name__ == '__main__':
    logging_config()
    unittest.main()
