import aioredis
from aioredis import create_sentinel
from aioredis.commands import Pipeline


class AIORedisHelper(object):
    def __init__(self, host: str, port: int, is_sentinel=False,
                 sentinel_service=None, password=None):
        self.host = host
        self.port = port
        self.is_sentinel = is_sentinel
        self.sentinel_service = sentinel_service
        self.password = password

        self.connection = None

    async def get_connection(self, is_read_only=False) -> \
            aioredis.ConnectionsPool:
        """
        Gets a StrictRedis connection for normal redis or for
        redis sentinel based upon redis mode in configuration.

        :type is_read_only: bool
        :param is_read_only: In case of redis sentinel,
        it returns connection to slave

        :return: Returns a StrictRedis connection
        """
        if self.connection is not None:
            return self.connection
        if self.is_sentinel:
            kwargs = dict()
            if self.password:
                kwargs["password"] = self.password
            sentinel = await create_sentinel([(self.host, self.port)], **kwargs)
            if is_read_only:
                connection = await sentinel.slave_for(self.sentinel_service)
            else:
                connection = await sentinel.master_for(self.sentinel_service)
        else:
            connection = await aioredis.create_redis(
                (self.host, self.port),
                password=self.password)
        self.connection = connection
        return connection

    async def get_atomic_connection(self) -> Pipeline:
        """
        Gets a pipeline for normal redis or for redis sentinel based
        upon redis mode in configuration

        :return: Returns a pipeline
        """
        connection = await self.get_connection()
        return connection.pipeline()
