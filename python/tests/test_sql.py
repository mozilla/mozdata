# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from mozdata import MozData


def test_mocked_sql(spark_fake):
    assert MozData(spark_fake).sql("query") == (("query",), {})
