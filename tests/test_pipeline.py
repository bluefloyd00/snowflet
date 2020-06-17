import time
import asyncio
import logging
import unittest
from unittest import mock
import snowflet.pipeline as pl


class TestExecute_parallel(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)

    def sleep_2_sec(self, num):
        time.sleep(2)
        a = 3 + 2
        return "ciao"

    def test_time_is_right(self):
        start_time = time.time()
        list_values = [
            {"num": "1"}, {"num": "2"}, {"num": "3"},
            {"num": "4"}, {"num": "5"}, {"num": "6"},
            {"num": "7"}, {"num": "8"}
            ]

        pl.execute_parallel(
                    self.sleep_2_sec,
                    list_values)

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
                    self.bug,
                    [{"status": "ok"}, {"status": "ok"}, {"status": "error"}, {"status": "ok"}]
                )

if __name__ == '__main__':
    unittest.main()
