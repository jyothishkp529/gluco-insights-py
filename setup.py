
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructType, StructField


#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

#import sparkhelloworld


if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
    readme = f.read()

packages = [
    'pipeline',
]

package_data = {
}

requires = [
]

classifiers = [
]

setup(
    name='gluco-insights',
    version=sparkhelloworld.__version__,
    description='A pyspark example application',
    long_description=readme,
    packages=packages,
    package_data=package_data,
    install_requires=requires,
    author=gluco-insights.__author__,
    url='https://github.com/jyothishkp529',
    license='MIT',
    classifiers=classifiers,
)