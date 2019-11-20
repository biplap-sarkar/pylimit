import redis
from redis.sentinel import Sentinel
from redis.client import Pipeline
import redis.client


class RedisHelper(object):
    def __init__(self, host: str, port: int, is_sentinel=False,
                 sentinel_service=None, password=None):
        self.host = host
        self.port = port
        self.is_sentinel = is_sentinel
        self.sentinel_service = sentinel_service
        self.password = password

        self.connection = None
        self.get_connection()  # Ensure connection is established

    def get_connection(self, is_read_only=False) -> redis.StrictRedis:
        """
        Gets a StrictRedis connection for normal redis or for redis sentinel
         based upon redis mode in configuration.

        :type is_read_only: bool
        :param is_read_only: In case of redis sentinel, it returns connection
         to slave

        :return: Returns a StrictRedis connection
        """
        if self.connection is not None:
            return self.connection

        if self.is_sentinel:
            kwargs = dict()
            if self.password:
                kwargs["password"] = self.password
            sentinel = Sentinel([(self.host, self.port)], **kwargs)
            if is_read_only:
                connection = sentinel.slave_for(self.sentinel_service,
                                                decode_responses=True)
            else:
                connection = sentinel.master_for(self.sentinel_service,
                                                 decode_responses=True)
        else:
            connection = redis.StrictRedis(host=self.host, port=self.port,
                                           decode_responses=True,
                                           password=self.password)
        self.connection = connection
        return connection

    def get_atomic_connection(self) -> Pipeline:
        """
        Gets a Pipeline for normal redis or for redis sentinel based upon
         redis mode in configuration

        :return: Returns a Pipeline object
        """
        return self.get_connection().pipeline(True)
