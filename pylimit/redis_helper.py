import redis
from redis.sentinel import Sentinel
from redis.client import StrictPipeline
import redis.client
from rediscluster import StrictRedisCluster


class RedisHelper(object):
    def __init__(self, host: str, port: int,
        is_sentinel=False,
        sentinel_service=None,
        password=None,
        is_cluster=False,
        decode_responses=True,
        skip_full_coverage_check=True):
        self.host = host
        self.port = port
        self.is_sentinel = is_sentinel
        self.sentinel_service = sentinel_service
        self.password = password
        # New settings for cluster connection. 
        self.is_cluster = is_cluster
        self.decode_responses = decode_responses
        self.skip_full_coverage_check = skip_full_coverage_check

        self.connection = None
        self.get_connection()  # Ensure connection is established

    def get_connection(self, is_read_only=False) -> redis.StrictRedis:
        """
        Gets a StrictRedis connection for normal redis or for redis sentinel based upon redis mode in configuration.

        :type is_read_only: bool
        :param is_read_only: In case of redis sentinel, it returns connection to slave

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
                decode_responses=self.decode_responses)
            else:
                connection = sentinel.master_for(self.sentinel_service, decode_responses=True)
        elif self.is_cluster:
            # handle connection string for aws elastic cache redis cluster
            cluster_endpoint = {"host": self.host, "port": self.port}
            connection = StrictRedisCluster(startup_nodes=[cluster_endpoint],
                                            decode_responses=self.decode_responses,
                                            skip_full_coverage_check=self.skip_full_coverage_check)
        else:
            connection = redis.StrictRedis(host=self.host, port=self.port,
                                           decode_responses=self.decode_responses,
                                           password=self.password)
        self.connection = connection
        return connection

    def get_atomic_connection(self) -> StrictPipeline:
        """
        Gets a StrictPipeline for normal redis or for redis sentinel based upon redis mode in configuration

        :return: Returns a StrictPipeline object
        """
        return self.get_connection().pipeline(True)
