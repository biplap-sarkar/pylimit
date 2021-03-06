# pylimit
A distributed rate limiting library for python using leaky bucket algorithm and Redis

# Prerequisites
This library makes use of Redis and needs it as a prerequisite. Redis sentinel is also supported.
Make sure that Redis server is running before using this library

# Installation

```
pip install pylimit
```

# Example
For this example, let's assume that redis hostname is 'localhost' and port number is 6379
Suppose you want to limit the number of api calls to be 1000 per minute. It can be done by the following steps.

1.) Import the library
```
from pylimit import PyRateLimit
```

2.) Initialize the library
```
PyRateLimit.init(redis_host="localhost", redis_port=6379)
```

3.) Create a rate limit namespace
```
limit = PyRateLimit(period=60,      # rate limit period in seconds
                    limit=100)      # no of attempts in the time period
```

4.) Record ant attempt and check if it is allowed or not
```
is_allowed = limit.attempt('api_count')   # will return true if number of attempts
                                          # are less than or equal to 1000 in last 1 minute,
                                          # false otherwise
```

5.) In order to check if a namespace is already rate limited or not
```
is_rate_limited = limit.is_rate_limited('api_count')  # will return true if this namespace is already rate limited, false otherwise
```

# Reference
https://engineering.classdojo.com/blog/2015/02/06/rolling-rate-limiter/
