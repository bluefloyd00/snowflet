import os
import unittest
from snowflet.lib import read_sql
from snowflet.lib import logging_config
from snowflet.lib import extract_args
from snowflet.lib import apply_kwargs
from snowflet.lib import strip_table
from snowflet.lib import add_database_id_prefix



class StringFunctions(unittest.TestCase):
    """ Test """
    def test_strip_table(self):        
        """ Test """
        self.assertEqual(
            strip_table(table_name='"db"."schema"."table"'),
            "table",
            "does not return the table"
        )


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
                                    "database": "test",              
                                },                      
                            },
                            {
                                "table_desc": "table2",
                                "create_table": {
                                    "table_id": "table2",
                                    "database": "test",                                    
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
                                "database": "1234_test",              
                            },                      
                        },
                        {
                            "table_desc": "table2",
                            "create_table": {
                                "table_id": "table2",
                                "database": "1234_test",                                    
                            },                                    
                        }
                    ]                
                },
                "prefix properly added to database"
            )

if __name__ == "__main__":
    logging_config()
    unittest.main()
