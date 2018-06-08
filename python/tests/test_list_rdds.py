# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from mozdata import MozData


def test_list_rdds(spark_fake, rdd):
    rdds = MozData(spark_fake).list_rdds()
    assert(len(rdds) == 1)
    assert(sorted(rdds[0].items()) == [
        ("bucket", "net-mozaws-prod-us-west-2-pipeline-metadata"),
        ("metadata_prefix", "test"),
        ("name", "test"),
        ("prefix", "test"),
    ])
