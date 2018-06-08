# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from pyspark.sql import Row


def test_read_ad_hoc_tables(ad_hoc_table_v0, ad_hoc_table_v1, api, spark):
    table_name, owner = "read_table", "read_table"
    v0 = spark.read.csv(ad_hoc_table_v0).collect()
    v1 = spark.read.csv(ad_hoc_table_v1).collect()
    assert(api.read_table(
        table_name=table_name,
        owner=owner,
        extra_read_config=lambda r: r.format("csv"),
    ).collect() == v1)
    assert(api.read_table(
        table_name=table_name,
        owner=owner,
        version="v1",
        extra_read_config=lambda r: r.format("csv"),
    ).collect() == v1)
    assert(api.read_table(
        table_name=table_name,
        owner=owner,
        version="v0",
        extra_read_config=lambda r: r.format("csv"),
    ).collect() == v0)


def test_read_undefined_global_tables(api, global_table_v2, global_table_v3,
                                      spark):
    table_name = "read_table"
    v2 = spark.read.csv(global_table_v2).collect()
    v3 = spark.read.csv(global_table_v3).collect()
    assert(api.read_table(
        table_name=table_name,
        extra_read_config=lambda r: r.format("csv"),
    ).collect() == v3)
    assert(api.read_table(
        table_name=table_name,
        version="v3",
        extra_read_config=lambda r: r.format("csv"),
    ).collect() == v3)
    assert(api.read_table(
        table_name=table_name,
        version="v2",
        extra_read_config=lambda r: r.format("csv"),
    ).collect() == v2)


def test_read_defined_global_table(api, global_table_v3, spark):
    table_name = "read_table"
    spark.sql("""
         CREATE EXTERNAL TABLE `%s`(`a` int)
         STORED AS TEXTFILE
         LOCATION '%s'
    """ % (table_name, global_table_v3))
    assert(api.read_table(table_name).collect() == [Row(a=3)])
