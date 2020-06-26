import os
import unittest
from snowflet.lib import read_sql
from snowflet.lib import logging_config
from snowflet.lib import extract_args
from snowflet.lib import apply_kwargs
from snowflet.lib import strip_table
from snowflet.lib import extract_tables_from_query
from snowflet.lib import add_database_id_prefix
from snowflet.lib import is_table
from snowflet.lib import add_table_prefix_to_sql


class StringFunctions(unittest.TestCase):
    """ Test """
    def test_strip_table(self):        
        """ Test """
        self.assertEqual(
            strip_table(table_name='"db"."schema"."table"'),
            '"db.schema.table"',
            "strip_table: wrong table name"
        )
    def test_extract_tables_from_query(self):
        """ Test """
        self.assertEqual(
            extract_tables_from_query(sql_query=""" select a,b,c from "db"."schema"."table" and db.schema.table not "schema"."table" """),
            [  '"db"."schema"."table"',  'db.schema.table' ],
            "does not extract the tables properly"
        )

class TableFunctions(unittest.TestCase):
    """ Test """
    def test_is_table(self):
        self.assertTrue( 
            is_table( word='"db"."test"."table1"' ,sql=""" select a.* from "db"."test"."table1"  a left join db.test.table2 b on a.id=b.id left join db."test".table3 c on b.id = c.id """),
            "select: ok"
        )
        self.assertTrue( 
            is_table( word='"db"."test"."table4"' ,sql=""" create table "db"."test"."table4" as select a.* from "db"."test"."table1"  a left join db.test.table2 b on a.id=b.id left join db."test".table3 c on b.id = c.id """),
            "create - select: ok"
        )

    def test_add_table_prefix_to_sql(self):
        self.assertEqual( 
            add_table_prefix_to_sql(  
                sql=""" select a.* from "db1"."test"."table1"  a left join db2.test.table2 b on a.id=b.id left join db3."test".table3 c on b.id = c.id """,
                prefix="CLONE_1003"
            ),
            """ select a.* from "CLONE_1003_db1"."test"."table1"  a left join "CLONE_1003_db2".test.table2 b on a.id=b.id left join "CLONE_1003_db3"."test".table3 c on b.id = c.id """,
            "quert: ok"
        )
        

    # def test_extract_tables(self):
    #     self.assertEqual( 
    #         extract_tables(""" select a.* from "db"."test"."table1" and db.test.table2 and db."test".table3 """),
    #         ["db.test.table1", "db.test.table2", "db.test.table3"],
    #         "multiple tables, mix double quotes and not"
    #     )
    #     self.assertEqual( 
    #         extract_tables(""" select a.* from "db"."test"."table1" and db.test.table2 and db."test".table1 """),
    #         ["db.test.table1", "db.test.table2"],
    #         "returned unique values"
    #     )

        


class ReadSql(unittest.TestCase):
    """ Test """
    def test_class_read_sql_file(self):        
        """ Test """
        sql = read_sql(
            file="tests/sql/read_sql.sql",
            param1="type",
            param2="300",
            param3="shipped_date",
            param4='trying'
        )
        self.assertEqual(
            sql,
            'select type, shipped_date from "db_test"."schema_test"."table1" where amount > 300',
            "read_sql unit test"
        )
        sql = read_sql(
            file="tests/sql/read_sql.sql"
            )
        self.assertTrue(
            sql == 'select {param1}, {param3} from "db_test"."schema_test"."table1" where amount > {param2}',
            "read_sql unit test no opt parameters"
        )

        with self.assertRaises(KeyError):
            read_sql(
                file="tests/sql/read_sql.sql",
                list_of_dedicated_keywords='20200101'
            )

    def test_class_read_sql_query(self):        
        """ Test """
        sql = read_sql(
            query='select {param1}, {param3} from "db_test"."schema_test"."table1" where amount > {param2}',
            param1="type",
            param2="300",
            param3="shipped_date",
            param4='trying'
        )
        self.assertEqual(
            sql,
            'select type, shipped_date from "db_test"."schema_test"."table1" where amount > 300',
            "read_sql unit test"
        )
        sql = read_sql(
            file="tests/sql/read_sql.sql"
            )
        self.assertTrue(
            sql == 'select {param1}, {param3} from "db_test"."schema_test"."table1" where amount > {param2}',
            "read_sql unit test no opt parameters"
        )

        with self.assertRaises(KeyError):
            read_sql(
                file="tests/sql/read_sql.sql",
                list_of_dedicated_keywords='20200101'
            )

class FunctionsInLib(unittest.TestCase):
    """
    Unittest class for lib functions
    """

    def test_extract_args_1_param(self):
        content = [
                        {
                            "table_desc": "table1",
                            "create_table": {
                                "table_id": "table1",
                                "dataset_id": "test",              
                                "file": "tests/sql/table1.sql"
                            },
                            "pk": ["col1", "col2"],
                            "mock_data": "sql/table1_mocked.sql"
                        },
                        {
                            "table_desc": "table2",
                            "create_table": {
                                "table_id": "table2",
                                "dataset_id": "test",
                                "file": "tests/sql/table2.sql"
                            },
                            "pk": ["col1"],
                            "mock_data": "sql/table1_mocked.sql"
                        }
                    ]        

        self.assertEqual(
            extract_args(content, "pk"),
            [["col1", "col2"], ["col1"]],
            "extracted ok"
            )

        self.assertEqual(
            extract_args(content, "create_table"),
            [
                {
                    "table_id": "table1",
                    "dataset_id": "test",
                    "file": "tests/sql/table1.sql"
                },
                {
                    "table_id": "table2",
                    "dataset_id": "test",
                    "file": "tests/sql/table2.sql"
                }
            ],
            "extracted ok"
            )

    def test_add_database_id_prefix(self):
        self.yaml = {
                        "desc": "test",
                        "tables":
                        [
                            {
                                "table_desc": "table1",
                                "create_table": {
                                    "table_id": "table1",
                                    "database_id": "test",              
                                },                      
                            },
                            {
                                "table_desc": "table2",
                                "create_table": {
                                    "table_id": "table2",
                                    "database_id": "test",                                    
                                },                                    
                            }
                        ]
                    }
        add_database_id_prefix(
                    self.yaml,
                    prefix='1234'
                )
        self.assertEqual(
                self.yaml
                , 
                {
                    "desc": "test",
                    "tables":
                    [
                        {
                            "table_desc": "table1",
                            "create_table": {
                                "table_id": "table1",
                                "database_id": "1234_test",              
                            },                      
                        },
                        {
                            "table_desc": "table2",
                            "create_table": {
                                "table_id": "table2",
                                "database_id": "1234_test",                                    
                            },                                    
                        }
                    ]                
                },
                "prefix properly added to database"
            )

if __name__ == "__main__":
    logging_config()
    unittest.main()
