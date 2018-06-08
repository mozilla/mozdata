# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from os.path import join
from pyspark.sql import Row
from conftest import rm


def test_write_ad_hoc_tables(ad_hoc_tables_dir, api, spark):
    owner, table_name = "write_table", "write_table"
    uri = join(ad_hoc_tables_dir, owner, table_name)
    # make sure table doesn't exist
    rm(uri)
    # write new table
    api.write_table(
        df=spark.sql("SELECT 'a', 'b'"),
        table_name=table_name,
        owner=owner,
    )
    assert(
        spark.read.parquet(join(uri, "v1")).collect() == [Row(a="a", b="b")]
    )
    # write new version
    api.write_table(
        df=spark.sql("SELECT 'c', 'd'"),
        table_name=table_name,
        owner=owner,
        version="v2",
    )
    # append to latest
    api.write_table(
        df=spark.sql("SELECT 'e' AS c, 'f' AS d"),
        table_name=table_name,
        owner=owner,
        extra_write_config=lambda w: w.mode("append"),
    )
    assert(sorted(spark.read.parquet(join(uri, "v2")).collect()) == [
        Row(c="c", d="d"),
        Row(c="e", d="f"),
    ])


def test_write_table_uri(ad_hoc_tables_dir, api, spark):
    table_name = "write_table"
    uri = join(ad_hoc_tables_dir, "uri", table_name)
    # make sure table doesn't exist
    rm(uri)
    # write only uri
    api.write_table(
        df=spark.sql("SELECT 'a', 'b'"),
        table_name=table_name,
        uri=uri,
    )
    assert(spark.read.parquet(uri).collect() == [Row(_c0="a", _c1="b")])


def test_write_undefined_global_table(api, global_tables_dir, spark):
    table_name, version = "write_table", "v1"
    uri = join(global_tables_dir, table_name, version)
    # make sure table doesn't exist
    rm(join(global_tables_dir, table_name))
    # write
    api.write_table(
        df=spark.sql("SELECT 'a', 'b'"),
        table_name=table_name,
        version=version,
    )
    assert(spark.read.parquet(uri).collect() == [Row(_c0="a", _c1="b")])


def test_write_defined_global_table(api, global_tables_dir, spark):
    table_name, version = "write_table", "v2"
    uri = join(global_tables_dir, table_name, version)
    # create table
    spark.sql("""
         CREATE EXTERNAL TABLE `%s_%s`(`a` int)
         STORED AS PARQUET
         LOCATION '%s'
    """ % (table_name, version, uri))
    # make sure table doesn't exist
    rm(uri)
    # write
    api.write_table(
        df=spark.sql("SELECT 0 AS a"),
        table_name=table_name,
        version=version,
        metadata_update_methods=["sql_refresh"],
    )
    assert(spark.sql(
        "SELECT * FROM `%s_%s`" % (table_name, version)
    ).collect() == [Row(a=0)])


def test_write_and_overwrite_partitioned_global_table(api, global_tables_dir,
                                                      spark):
    table_name, version = "write_table", "v3"
    uri = join(global_tables_dir, table_name, version)
    # create table
    spark.sql("""
         CREATE EXTERNAL TABLE `%s_%s`(`a` int)
         PARTITIONED BY (`b` string)
         STORED AS PARQUET
         LOCATION '%s'
    """ % (table_name, version, uri))
    spark.sql("""
         CREATE VIEW `%s`
         AS SELECT * FROM `%s_%s`
    """ % (table_name, table_name, version))
    # make sure table doesn't exist
    rm(uri)
    # write
    api.write_table(
        df=spark.sql("SELECT 0 AS a, 'b'"),
        table_name=table_name,
        version=version,
        extra_write_config=lambda w: w.partitionBy("b"),
    )
    assert(
        spark.sql("SELECT * FROM " + table_name).collect() == [Row(a=0, b="b")]
    )
    # overwrite
    api.write_table(
        df=spark.sql("SELECT 1 AS a, 'b'"),
        table_name=table_name,
        version=version,
        extra_write_config=lambda w: w.mode("overwrite").partitionBy("b"),
    )
    assert(
        spark.sql("SELECT * FROM " + table_name).collect() == [Row(a=1, b="b")]
    )


def test_write_ad_hoc_table_partitions(ad_hoc_tables_dir, api, spark):
    owner, table_name = "write_partition", "write_partition"
    uri = join(ad_hoc_tables_dir, owner, table_name)
    # make sure table doesn't exist
    rm(uri)
    # write new table
    api.write_table(
        df=spark.sql("SELECT 'a'"),
        table_name=table_name,
        partition_values=[("b", "b"), ("c", "c")],
        owner=owner,
    )
    # write new version
    api.write_table(
        df=spark.sql("SELECT 'd'"),
        table_name=table_name,
        partition_values=[("e", "e"), ("f", "f")],
        owner=owner,
        version="v2",
    )
    # append to latest
    api.write_table(
        df=spark.sql("SELECT 'g' AS d, 'f'"),
        table_name=table_name,
        partition_values=[("e", "e")],
        owner=owner,
        extra_write_config=lambda w: w.mode("append").partitionBy("f"),
    )
    # append new dynamic partition
    api.write_table(
        df=spark.sql("SELECT 'h' AS d, 'j' AS f"),
        table_name=table_name,
        partition_values=[("e", "i")],
        owner=owner,
        extra_write_config=lambda w: w.mode("append").partitionBy("f"),
    )
    # append new static partition
    api.write_table(
        df=spark.sql("SELECT 'k' AS d"),
        table_name=table_name,
        partition_values=[("e", "l"), ("f", "m")],
        owner=owner,
    )
    assert(sorted(spark.read.parquet(join(uri, "v2")).collect()) == [
        Row(d="d", e="e", f="f"),
        Row(d="g", e="e", f="f"),
        Row(d="h", e="i", f="j"),
        Row(d="k", e="l", f="m"),
    ])


def test_write_partition_uris(ad_hoc_tables_dir, api, spark):
    table_name = "write_partition"
    uri = join(ad_hoc_tables_dir, "uri", table_name)
    # make sure table doesn't exist
    rm(uri)
    # write only uri
    api.write_table(
        df=spark.sql("SELECT 'a'"),
        table_name=table_name,
        uri=join(uri, "b=b", "c=c"),
    )
    # write mixed uri & partition spec
    api.write_table(
        df=spark.sql("SELECT 'd' AS a"),
        table_name=table_name,
        partition_values=[("c", "f")],
        uri=join(uri, "b=e"),
    )
    # write all partitions in partition spec
    api.write_table(
        df=spark.sql("SELECT 'g' AS a"),
        table_name=table_name,
        partition_values=[("b", "h"), ("c", "i")],
        uri=uri,
    )
    assert(sorted(spark.read.parquet(uri).collect()) == [
        Row(_c0="a", b="b", c="c"),
        Row(_c0="d", b="e", c="f"),
        Row(_c0="g", b="h", c="i"),
    ])


def test_write_undefined_global_table_partition(api, global_tables_dir, spark):
    table_name, version = "write_partition", "v1"
    uri = join(global_tables_dir, table_name, version)
    # make sure table doesn't exist
    rm(join(global_tables_dir, table_name))
    # write
    api.write_table(
        df=spark.sql("SELECT 'a'"),
        table_name=table_name,
        partition_values=[("b", "b")],
        version=version,
    )
    assert(spark.read.parquet(uri).collect() == [Row(_c0="a", b="b")])


def test_write_and_overwrite_global_table_partitions(api, global_tables_dir,
                                                     spark):
    table_name, version = "write_partition", "v3"
    uri = join(global_tables_dir, table_name, version)
    # create table
    spark.sql("""
         CREATE EXTERNAL TABLE `%s_%s`(`a` int)
         PARTITIONED BY (`b` string, `c` string)
         STORED AS PARQUET
         LOCATION '%s'
    """ % (table_name, version, uri))
    spark.sql("""
         CREATE VIEW `%s`
         AS SELECT * FROM `%s_%s`
    """ % (table_name, table_name, version))
    # make sure table doesn't exist
    rm(uri)
    # write
    api.write_table(
        df=spark.sql("SELECT 0 AS a"),
        table_name=table_name,
        partition_values=[("b", "b"), ("c", "c")],
        version=version,
    )
    assert(
        spark.sql("SELECT * FROM " + table_name).collect() ==
        [Row(a=0, b="b", c="c")]
    )
    # overwrite
    api.write_table(
        df=spark.sql("SELECT 1 AS a, 'c'"),
        table_name=table_name,
        partition_values=[("b", "b")],
        version=version,
        extra_write_config=lambda w: w.mode("overwrite").partitionBy("c"),
    )
    assert(
        spark.sql("SELECT * FROM " + table_name).collect() ==
        [Row(a=1, b="b", c="c")]
    )
