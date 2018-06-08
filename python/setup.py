#!/user/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup
from os.path import isfile

# read the version file in the package
version_file = "VERSION" if isfile("VERSION") else "../VERSION"
with open("VERSION", 'r') as f:
    VERSION = f.read().strip()

setup(
    name='mozdata',
    version=VERSION.split('-')[0],
    author='Daniel Thorn',
    author_email='daniel@relud.com',
    description='Easily read and write Mozilla data',
    url='https://github.com/mozilla/mozdata',
    packages=[
        'mozdata',
        'pyspark.jars'
    ],
    install_requires=[
        'pyspark',
        'python_moztelemetry',
    ],
    extras_require={
        'dev': [
            'moto',
            'pytest-flake8',
            'pytest',
            'tox',
        ],
    },
    include_package_data=True,
    package_dir={
        'mozdata': 'src',
        'pyspark.jars': 'deps/jars',
    },
    package_data={
        'pyspark.jars': ['*.jar'],
    },
)
