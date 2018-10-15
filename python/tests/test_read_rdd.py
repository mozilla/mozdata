# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from mozdata import MozData
import mock


def test_read_rdd_calls_records(spark_fake, rdd):
    # override Dataset.records to return the dataset because we don't need
    # to test that .records() works and the mock.patch for boto3 breaks it
    def records(self, *args, **kwargs):
        return self
    with mock.patch('moztelemetry.dataset.Dataset.records', new=records):
        ds = MozData(spark_fake).read_rdd("test")
        assert(ds.bucket == "net-mozaws-prod-us-west-2-pipeline-metadata")
        assert(ds.schema == ["key"])
        assert(ds.prefix == "test")
        assert(ds.clauses == {})
