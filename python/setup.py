#!/user/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup

try:
    # read the version file in the package
    with open("VERSION", 'r') as f:
        VERSION = f.read().strip()
except IOError:
    # no version file
    VERSION = "0.0-SNAPSHOT"

setup(
    name='mozdata',
    version=VERSION.split('-')[0],
    author='Daniel Thorn',
    author_email='daniel@relud.com',
    description='Easily read and write Mozilla data',
    url='https://github.com/mozilla/mozdata',
    packages=[
        'mozdata',
    ],
    install_requires=[
        'pyspark',
        'requests',
    ],
    extras_require={
        'dev': [
            'codecov',
            'mock',
            'pytest-cov',
            'pytest-env',
            'pytest-flake8',
            'pytest-pythonpath',
            'pytest',
        ],
    },
    include_package_data=True,
    package_dir={
        'mozdata': 'src/mozdata',
    },
)
