from distutils.core import setup
setup(
  name = 'aiopylimit',
  packages = ['aiopylimit'],
  version = '0.1.1',
  description = 'A distributed rate limiting library for python using leaky bucket algorithm and Redis(asyncio version)',
  author='David Markey',
  author_email='david@dmarkey.com',
  url='https://github.com/dmarkey/aiopylimit',
  keywords=['rate limiting', 'throttle', 'redis'],
  classifiers=[],
)
