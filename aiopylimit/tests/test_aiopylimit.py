from aiopylimit import AIOPyRateLimit
from aiopylimit import AIOPyRateLimitException
import asynctest
import asyncio


class TestPyLimit(asynctest.TestCase):
    async def test_exception(self):
        limit = AIOPyRateLimit()
        await self.assertAsyncRaises(AIOPyRateLimitException,
                                     limit.attempt('test_namespace'))

    async def test_throttle(self):
        AIOPyRateLimit.init(redis_host="localhost", redis_port=6379,
                            force_new_connection=True)
        limit = AIOPyRateLimit()
        limit.create(10,  # rate limit period in seconds
                     10)  # no of attempts in the time period
        for x in range(0, 20):
            await asyncio.sleep(.5)
            if x < 10:
                self.assertTrue(await limit.attempt('test_namespace'))
            else:
                self.assertFalse(await limit.attempt('test_namespace'))
        await asyncio.sleep(6)
        self.assertTrue(await limit.attempt('test_namespace'))

    async def test_peek(self):
        AIOPyRateLimit.init(redis_host="localhost", redis_port=6379,
                            force_new_connection=True)
        limit = AIOPyRateLimit()
        limit.create(10,  # rate limit period in seconds
                     10)  # no of attempts in the time period
        for x in range(0, 10):
            self.assertTrue(await limit.attempt('test_namespace2'))
        self.assertTrue(await limit.is_rate_limited('test_namespace2'))
        await asyncio.sleep(10)
        self.assertFalse(await limit.is_rate_limited('test_namespace2'))
