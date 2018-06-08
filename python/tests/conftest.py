# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from pytest import fixture


class SparkFake:
    sparkContext = None
    _jvm = None

    def sql(self, *args, **kwargs):
        return args, kwargs


@fixture
def spark_fake():
    return SparkFake()
