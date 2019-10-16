from setuptools import setup
from setuptools import find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='aws_utils',
      version='0.0.0',
      description='A collection of tools to make it easier to incorporate S3 assets to your code',
      long_description=long_description,
      author='Audrey Beard',
      author_email='audrey.s.beard@gmail.com',
      packages=find_packages(),
      install_requires=['boto3',
                        'awscli'],
      url='https://github.com/AudreyBeard/netdev',
      changelog={'0.0.0': 'Beta',
                 }
      )
