import unittest
from snowflet.lib import logging_config
from snowflet.lib import read_sql


class test_read_sql(unittest.TestCase):
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


if __name__ == "__main__":
    logging_config()
    unittest.main()
