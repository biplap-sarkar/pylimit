import time

from pylimit.redis_helper import RedisHelper
from pylimit.pyratelimit_exception import PyRateLimitException


class PyRateLimit(object):
    redis_helper = None     # type: RedisHelper

    def __init__(self, period: int, limit: int):
        """
        :param period: Rate limiting period in seconds
        :type period: int

        :param limit: Number of attempts permitted by rate limiting within a
         given period
        :type limit: int
        """
        self.period = period      # type: int
        self.limit = limit       # type: int

    @classmethod
    def init(cls, redis_host: str, redis_port: int, is_sentinel_redis=False,
             redis_sentinel_service="mymaster",
             redis_password=None):
        """
        Initializes redis connection

        :param redis_host: Hostname of redis server
        :type redis_host: str

        :param redis_port: Port at which redis server is listening
        :type redis_port:int

        :param is_sentinel_redis: Parameter indicating if redis server is a
        sentinel server. Default is false
        :type is_sentinel_redis: bool

        :param redis_sentinel_service: If redis server is a sentinel server,
         service name for redis sentinel
        :type redis_sentinel_service: str

        :param redis_password: Password for redis connection
        :type redis_password: str

        """
        if not cls.redis_helper:
            cls.redis_helper = RedisHelper(
                host=redis_host, port=redis_port,
                is_sentinel=is_sentinel_redis,
                sentinel_service=redis_sentinel_service,
                password=redis_password)

    def __can_attempt(self, namespace: str, add_attempt=True) -> bool:
        """
        Checks if a namespace is rate limited or not with including/excluding
         the current call

        :param namespace: Rate limiting namespace
        :type namespace: str

        :param add_attempt: Boolean value indicating if the current call
        should be considered as an attempt or not
        :type add_attempt: bool

        :return: Returns true if attempt can go ahead under current rate
        limiting rules, false otherwise
        """
        can_attempt = False

        if not PyRateLimit.redis_helper:
            raise PyRateLimitException("Redis Connection information not provided")

        connection = PyRateLimit.redis_helper.get_atomic_connection()

        current_time = int(round(time.time() * 1000000))
        old_time_limit = current_time - (self.period * 1000000)
        connection.zremrangebyscore(namespace, 0, old_time_limit)

        connection.expire(namespace, self.period)

        if add_attempt:
            current_count = 0
            connection.zadd(namespace, current_time, current_time)
        else:
            current_count = 1   # initialize at 1 to compensate the case that this attempt is not getting counted

        connection.zcard(namespace)
        redis_result = connection.execute()

        current_count += redis_result[-1]

        if current_count <= self.limit:
            can_attempt = True

        return can_attempt

    def attempt(self, namespace: str):
        """
        Records an attempt and returns true of false depending on whether
        attempt can go through or not

        :param namespace: Rate limiting namespace
        :type namespace: str

        :return: Returns true if attempt can go ahead under current rate
        limiting rules, false otherwise
        """
        return self.__can_attempt(namespace=namespace)

    def is_rate_limited(self, namespace: str) -> bool:
        """
        Checks if a namespace is already rate limited or not without making
        any additional attempts

        :param namespace:  Rate limiting namespace
        :type namespace: str

        :return:    Returns true if attempt can go ahead under current rate
        limiting rules, false otherwise
        """
        return not self.__can_attempt(namespace=namespace, add_attempt=False)
