from pylimit import PyRateLimit
from pylimit import PyRateLimitException
import unittest
import time


class TestPyLimit(unittest.TestCase):
    def test_exception(self):
        limit = PyRateLimit()
        self.assertRaises(PyRateLimitException, limit.attempt,
                          'test_namespace')

    def test_throttle(self):
        PyRateLimit.init(redis_host="localhost", redis_port=6379)
        limit = PyRateLimit()
        limit.create(10,                    # rate limit period in seconds
                     10)                    # no of attempts in the time period
        for x in range(0, 20):
            time.sleep(.5)
            if x < 10:
                self.assertTrue(limit.attempt('test_namespace'))
            else:
                self.assertFalse(limit.attempt('test_namespace'))
        time.sleep(6)
        self.assertTrue(limit.attempt('test_namespace'))

    def test_peek(self):
        PyRateLimit.init(redis_host="localhost", redis_port=6379)
        limit = PyRateLimit()
        limit.create(10,                    # rate limit period in seconds
                     10)                    # no of attempts in the time period
        for x in range(0, 10):
            self.assertTrue(limit.attempt('test_namespace2'))
        self.assertTrue(limit.is_rate_limited('test_namespace2'))
        time.sleep(10)
        self.assertFalse(limit.is_rate_limited('test_namespace2'))

