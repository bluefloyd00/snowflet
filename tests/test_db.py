from snowflet.db import DBExecutor as db
from snowflet.lib import logging_config
import unittest



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

if __name__ == "__main__":
    logging_config()
    unittest.main()
