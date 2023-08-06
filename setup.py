from distutils.core import setup
setup(
  name = 'pylimit',
  packages = ['pylimit'],
  version = '1.0.0',
  description = 'A distributed rate limiting library for python using leaky bucket algorithm and Redis',
  author='Biplap Sarkar',
  author_email='biplap.sarkar@gmail.com',
  url='https://github.com/biplap-sarkar/pylimit',
  download_url='https://github.com/biplap-sarkar/pylimit/archive/v1.0.0.tar.gz',
  keywords=['rate limiting', 'throttle', 'redis'],
  classifiers=[],
)
