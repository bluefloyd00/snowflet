from snowflet.db import DBExecutor as db
from snowflet.lib import logging_config
import pandas as pd
import unittest
from snowflet.lib import df_assert_equal

class DBWriteTable(unittest.TestCase):

    """ Test """
    def setUp(self):

        """ Test """
        self.db = db() 
        self.db.create_database(database_id="test_write_table")
        self.db.create_schema(database_id="test_write_table", schema_id="test")
        
        

    def tearDown(self):
        """ Test """
        try: 
            self.db.validate_connection() 
        except:
            self.db = db()
            
        self.db.delete_database(database_id="test_write_table")
        self.db.close()
       
    # def test_write_table_too_many_arguments(self):
    #     with self.assertRaises(Exception):
    #         self.db.write_table(
    #             database_id="test_write_table",
    #             schema_id="test",
    #             table_id="test_write_table_too_many_arguments",
    #             query="query",
    #             file_query="path_to_query"
    #         )

    # def test_write_table_no_arguments(self):
    #     with self.assertRaises(Exception):
    #         self.db.write_table(
    #             database_id="test_write_table",
    #             schema_id="test",
    #             table_id="test_write_table_no_arguments"                
    #         )

    # def test_write_table_table_does_not_exists_no_ddl(self):
    #     pass
    # def test_write_table_table_does_not_exists_with_ddl(self):
    #     pass

    def test_write_table_table_exists_and_truncate(self):
        self.db.query_exec(query="create table test_write_table.test.test_write_table_table_exists_and_truncate as select 1 as col1")
        self.db.write_table(
                database_id="test_write_table",
                schema_id="test",
                table_id="test_write_table_no_arguments",
                query="select 2 as col1",
                truncate=True              
            )
        expected_df = pd.DataFrame(data={'col1': 2})
        returned_df = self.db.query_exec(query="select * from test_write_table.test.test_write_table_table_exists_and_truncate")
        df_assert_equal(expected_df, returned_df)

    # def test_write_table_table_exists_no_truncate(self):
    #     pass
    
        
# class DBExecutorUtilities(unittest.TestCase):

#     """ Test """
#     def setUp(self):

#         """ Test """
#         self.db = db() 
#         self.db.create_database(database_id="test_utilities")
#         self.db.create_schema(database_id="test_utilities", schema_id="test")
        

#     def tearDown(self):
#         """ Test """
#         self.db.delete_database(database_id="test_utilities")
#         self.db.close()
       
#     def test_table_exists(self):
#         self.db.query_exec(query="CREATE TABLE TEST_UTILITIES.TEST.table2 AS SELECT 1 as col1")
#         self.assertTrue(
#             self.db.table_exists(
#                 database_id="TEST_UTILITIES",
#                 schema_id="TEST",
#                 table_id="table2"
#             ),
#             "issue with table_exists method, table2 not found"
#         )

#     def test_database_does_not_exist(self):

#         """ Test """
#         self.assertFalse(
#             self.db.database_exists("ciao")
#         )
#     def test_database_does_exists(self):

#         """ Test """
#         self.assertTrue(
#             self.db.database_exists("test_utilities")
#         )

# class DBExecutorValidateConnection(unittest.TestCase):

#     """ Test """
#     def setUp(self):

#         """ Test """
#         self.db = db()

#     def tearDown(self):
#         """ Test """
#         self.db.close()

#     def test_dataset_does_not_exists(self):

#         """ Test """
#         self.assertIsNotNone(
#             self.db.validate_connection()
#         )


# class DBExecutorQueryExec(unittest.TestCase):

#     """ Test """
#     def setUp(self):

#         """ Test """
#         self.db = db()
#         self.db.initiate_database_schema(database_id="TESTQUERYEXEC", schema_id='UNITTESTS')

#     def tearDown(self):

#         """ Test """
#         self.db.delete_database(database_id="TESTQUERYEXEC")
#         self.db.close()
        

#     def test_query_exec_create_table_read_df(self):

#         """ Test """
#         self.db.query_exec(
#             query="""create table "TESTQUERYEXEC"."UNITTESTS"."example" as select 1 as col1 union all select 2 as col1"""       
#         )
#         sql_df = self.db.query_exec(
#             query="""select * from "TESTQUERYEXEC"."UNITTESTS"."example" """,
#             return_df=True
#         )
#         expected_df = pd.DataFrame([1, 2], columns=['col1'])
#         df_assert_equal(expected_df, sql_df)


if __name__ == "__main__":
    logging_config()
    unittest.main()
