from snowflet.db import DBExecutor as db
from snowflet.lib import logging_config
import pandas as pd
import unittest
from snowflet.lib import df_assert_equal


def initiate_db(database):
    newdb = db()
    newdb.query_exec(
            query="create database {db}",
            db=database            
        )
    newdb.query_exec(
            query="use database  {db}",
            db=database            
        )
    newdb.query_exec(
            query="create schema unittests",
            db=database            
        )

def delete_db(database):
    newdb = db()
    newdb.query_exec(
            query="DROP DATABASE {db}",
            db=database            
        )

class DBExecutorValidateConnection(unittest.TestCase):

    """ Test """
    def setUp(self):

        """ Test """
        self.db = db()
        

    def tearDown(self):
  
        """ Test """
        self.db.close()
        

    def test_dataset_does_not_exists(self):

        """ Test """
        self.assertIsNotNone(
            self.db.validate_connection()
        )

class DBExecutorQueryExec(unittest.TestCase):

    """ Test """
    def setUp(self):

        """ Test """
        self.db = db()
        initiate_db(database="TestQueryExec")

    def tearDown(self):
  
        """ Test """
        self.db.close()
        delete_db(database="TestQueryExec")

    def test_query_exec_create_table_read_df(self):

        """ Test """
        self.db.query_exec(
            query="""create table "TESTQUERYEXEC"."UNITTESTS"."example" as select 1 as col1 union all select 2 as col1"""       
        )
        sql_df = self.db.query_exec(
            query="""select * from "TESTQUERYEXEC"."UNITTESTS"."example" """,
            return_df=True        
        )
        expected_df = pd.DataFrame([1,2], columns = ['col1']) 
        df_assert_equal(expected_df, sql_df)
        

if __name__ == "__main__":
    logging_config()
    unittest.main()
