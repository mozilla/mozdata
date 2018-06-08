# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from os.path import join
from pyspark.sql import Row
from conftest import rm


def test_list_global_tables(api, spark):
    assert not api.list_tables()
    spark.sql("SELECT 0").createOrReplaceTempView("table1")
    spark.sql("SELECT 0").createOrReplaceTempView("table2")
    assert(sorted(api.list_tables()) == ["table1", "table2"])


def test_list_ad_hoc_tables(ad_hoc_tables_dir, api, spark):
    owner = "list_tables"
    # make sure tables don't exist
    rm(join(ad_hoc_tables_dir, owner))
    assert(not api.list_tables(owner=owner))
    # create tables
    api.write_table(
        df=spark.sql("SELECT 0"),
        table_name="table1",
        owner=owner,
    )
    api.write_table(
        df=spark.sql("SELECT 0"),
        table_name="table2",
        owner=owner,
    )
    assert(sorted(api.list_tables(owner=owner)) == ["table1", "table2"])
