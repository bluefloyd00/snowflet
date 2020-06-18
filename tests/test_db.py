from snowflet.db import DBExecutor as db
from snowflet.lib import logging_config
import pandas as pd
import unittest
from snowflet.lib import df_assert_equal

class DBExecutorUtilities(unittest.TestCase):

    """ Test """
    def setUp(self):

        """ Test """
        self.db = db() 
        self.db.create_database(database_id="test_utilities")
        

    def tearDown(self):
        """ Test """
        self.db.delete_database(database_id="test_utilities")
        self.db.close()
       

    def test_database_does_not_exist(self):

        """ Test """
        self.assertFalse(
            self.db.database_exists("ciao")
        )
    def test_database_does_exists(self):

        """ Test """
        self.assertTrue(
            self.db.database_exists("test_utilities")
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
        self.db.initiate_database_schema(database_id="TESTQUERYEXEC", schema_id='UNITTESTS')

    def tearDown(self):

        """ Test """
        self.db.delete_database(database_id="TESTQUERYEXEC")
        self.db.close()
        

    def test_query_exec_create_table_read_df(self):

        """ Test """
        self.db.query_exec(
            query="""create table "TESTQUERYEXEC"."UNITTESTS"."example" as select 1 as col1 union all select 2 as col1"""       
        )
        sql_df = self.db.query_exec(
            query="""select * from "TESTQUERYEXEC"."UNITTESTS"."example" """,
            return_df=True
        )
        expected_df = pd.DataFrame([1, 2], columns=['col1'])
        df_assert_equal(expected_df, sql_df)


if __name__ == "__main__":
    logging_config()
    unittest.main()
