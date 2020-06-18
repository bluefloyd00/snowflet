import time
import logging
import unittest
# from unittest import mock
import snowflet.pipeline as pl
from snowflet.lib import logging_config

class TestTask(unittest.TestCase):
    def setUp(self):
        self.pipeline = pl.PipelineExecutor("tests/yaml/task.yaml")

    def test_map_object(self):
        tasks = [   
            {
                "desc": "running first task",
                "object": 'query_executor',
                "args": {"num": 1}
            },
            {
                "desc": "running second task",
                "object": 'query_executor',
                "args": {"num": 2}
            }
        ]
        expected_result = [   
            {
                "desc": "running first task",
                "object": self.pipeline.db.query_exec,
                "args": {"num": 1}
            },
            {
                "desc": "running second task",
                "object": self.pipeline.db.query_exec,
                "args": {"num": 2}
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
