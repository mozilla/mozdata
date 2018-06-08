# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from mozdata import MozData
from mozdata.mozdata import identity


def test_identity():
    x = {}
    assert identity(x) is x


def test_init_defaults(spark_fake):
    api = MozData(spark_fake)
    assert api.spark == spark_fake
    assert api.sc is None
    assert api.jvm is None
    assert (
        api.ad_hoc_tables_dir ==
        "s3://net-mozaws-prod-us-west-2-pipeline-analysis"
    )
    assert api.global_tables_dir == "s3://telemetry-parquet"
    assert api.default_metadata_update_methods == ["sql_repair", "sql_refresh"]
    assert api.read_config == identity
    assert api.write_config == identity
    assert api.telemetry_url is None
