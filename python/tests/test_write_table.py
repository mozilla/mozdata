# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from mozdata import MozData
from pyspark.sql import Row
import os
import os.path
import shutil


def rm(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.exists(path):
        os.remove(path)


def test_write_ad_hoc_table_partitions(api):
    owner, table_name = "write_partition", "write_partition"
    uri = os.path.join(api.ad_hoc_tables_dir, owner, table_name)
    uri_v1 = os.path.join(uri, "v1")
    uri_v2 = os.path.join(uri, "v2")
    # make sure table doesn't exist
    rm(uri)
    # write new table
    api.write_table(
        df=api.spark.sql("SELECT 'a'"),
        table_name=table_name,
        partition_values=[("b", "b"), ("c", "c")],
        owner=owner,
    )
    assert api.spark.read.parquet(uri_v1).collect() == [
        Row(a="a", b="b", c="c"),
    ]
    # write new version
    api.write_table(
        df=api.spark.sql("SELECT 'd'"),
        table_name=table_name,
        partition_values=[("e", "e"), ("f", "f")],
        owner=owner,
        version="v2",
    )
    # append to latest
    api.write_table(
        df=api.spark.sql("SELECT 'g' AS d, 'f'"),
        table_name=table_name,
        partition_values=[("e", "e")],
        owner=owner,
        extra_write_config=lambda w: w.mode("append").partitionBy("f"),
    )
    # append new dynamic partition
    api.write_table(
        df=api.spark.sql("SELECT 'h' AS d, 'j' AS f"),
        table_name=table_name,
        partition_values=[("e", "i")],
        owner=owner,
        extra_write_config=lambda w: w.mode("append").partitionBy("f"),
    )
    # append new static partition
    api.write_table(
        df=api.spark.sql("SELECT 'k' AS d"),
        table_name=table_name,
        partition_values=[("e", "l"), ("f", "m")],
        owner=owner,
    )
    assert sorted(api.spark.read.parquet(uri_v2).collect()) == [
        Row(d="d", e="e", f="f"),
        Row(d="g", e="e", f="f"),
        Row(d="h", e="i", f="j"),
        Row(d="k", e="l", f="m"),
    ]


def test_write_partition_uris(api):
    table_name = "write_partition"
    uri = os.path.join(api.ad_hoc_tables_dir, "uri", table_name)
    # make sure table doesn't exist
    rm(uri)
    # write only uri
    api.write_table(
        df=api.spark.sql("SELECT 'a'"),
        table_name=table_name,
        uri=os.path.join(uri, "b=b", "c=c"),
    )
    # write mixed uri & partition spec
    api.write_table(
        df=api.spark.sql("SELECT 'd' AS a"),
        table_name=table_name,
        partition_values=[("c", "f")],
        uri=os.path.join(uri, "b=e"),
    )
    # write all partitions in partition spec
    api.write_table(
        df=api.spark.sql("SELECT 'g' AS a"),
        table_name=table_name,
        partition_values=[("b", "h"), ("c", "i")],
        uri=uri,
    )
    assert sorted(api.spark.read.parquet(uri).collect()) == [
        Row(_c0="a", b="b", c="c"),
        Row(_c0="d", b="e", c="f"),
        Row(_c0="g", b="h", c="i"),
    ]


def test_write_undefined_global_table_partition(api):
    table_name, version = "write_partition", "v1"
    uri = os.path.join(api.global_tables_dir, table_name, version)
    # make sure table doesn't exist
    rm(os.path.join(api.global_tables_dir, table_name))
    # write
    api.write_table(
        df=api.spark.sql("SELECT 'a'"),
        table_name=table_name,
        partition_values=[("b", "b")],
        version=version,
    )
    assert api.spark.read.parquet(uri).collect() == [Row(_c0="a", b="b")]


def test_write_and_overwrite_global_table_partitions(api):
    table_name, version = "write_partition", "v3"
    uri = os.path.join(api.global_tables_dir, table_name, version)
    # create table
    api.spark.sql("""
         CREATE EXTERNAL TABLE `%s_%s`(`a` int)
         PARTITIONED BY (`b` string, `c` string)
         STORED AS PARQUET
         LOCATION '%s'
    """ % (table_name, version, uri))
    api.spark.sql("""
         CREATE VIEW `%s`
         AS SELECT * FROM `%s_%s`
    """ % (table_name, table_name, version))
    # make sure table doesn't exist
    rm(uri)
    # write
    api.write_table(
        df=api.spark.sql("SELECT 0 AS a"),
        table_name=table_name,
        partition_values=[("b", "b"), ("c", "c")],
        version=version,
    )
    assert api.spark.sql("SELECT * FROM " + table_name).collect() == [
        Row(a=0, b="b", c="c"),
    ]
    # overwrite
    api.write_table(
        df=api.spark.sql("SELECT 1 AS a, 'c'"),
        table_name=table_name,
        partition_values=[("b", "b")],
        version=version,
        extra_write_config=lambda w: w.mode("overwrite").partitionBy("c"),
    )
    assert api.spark.sql("SELECT * FROM " + table_name).collect() == [
        Row(a=1, b="b", c="c"),
    ]


def test_write_ad_hoc_tables(api):
    owner, table_name = "write_table", "write_table"
    uri = os.path.join(api.ad_hoc_tables_dir, owner, table_name)
    uri_v1 = os.path.join(uri, "v1")
    uri_v2 = os.path.join(uri, "v2")
    # make sure table doesn't exist
    rm(uri)
    # write new table
    api.write_table(
        df=api.spark.sql("SELECT 'a', 'b'"),
        table_name=table_name,
        owner=owner,
    )
    assert api.spark.read.parquet(uri_v1).collect() == [Row(a="a", b="b")]
    # write new version
    api.write_table(
        df=api.spark.sql("SELECT 'c', 'd'"),
        table_name=table_name,
        owner=owner,
        version="v2",
    )
    # append to latest
    api.write_table(
        df=api.spark.sql("SELECT 'e' AS c, 'f' AS d"),
        table_name=table_name,
        owner=owner,
        extra_write_config=lambda w: w.mode("append"),
    )
    assert sorted(api.spark.read.parquet(uri_v2).collect()) == [
        Row(c="c", d="d"),
        Row(c="e", d="f"),
    ]


def test_write_table_uri(api):
    table_name = "write_table"
    uri = os.path.join(api.ad_hoc_tables_dir, "uri", table_name)
    # make sure table doesn't exist
    rm(uri)
    # write only uri
    api.write_table(
        df=api.spark.sql("SELECT 'a', 'b'"),
        table_name=table_name,
        uri=uri,
    )
    assert api.spark.read.parquet(uri).collect() == [Row(_c0="a", _c1="b")]


def test_write_undefined_global_table(api):
    table_name, version = "write_table", "v1"
    uri = os.path.join(api.global_tables_dir, table_name, version)
    # make sure table doesn't exist
    rm(os.path.join(api.global_tables_dir, table_name))
    # write
    api.write_table(
        df=api.spark.sql("SELECT 'a', 'b'"),
        table_name=table_name,
        version=version,
    )
    assert api.spark.read.parquet(uri).collect() == [Row(_c0="a", _c1="b")]


def test_write_defined_global_table(api):
    table_name, version = "write_table", "v2"
    uri = os.path.join(api.global_tables_dir, table_name, version)
    # create table
    api.spark.sql("""
         CREATE EXTERNAL TABLE `%s_%s`(`a` int)
         STORED AS PARQUET
         LOCATION '%s'
    """ % (table_name, version, uri))
    # make sure table doesn't exist
    rm(uri)
    # write
    api.write_table(
        df=api.spark.sql("SELECT 0 AS a"),
        table_name=table_name,
        version=version,
        metadata_update_methods=["sql_refresh"],
    )
    assert api.spark.sql(
        "SELECT * FROM `%s_%s`" % (table_name, version)
    ).collect() == [Row(a=0)]


def test_write_and_overwrite_partitioned_global_table(api):
    table_name, version = "write_table", "v3"
    uri = os.path.join(api.global_tables_dir, table_name, version)
    # create table
    api.spark.sql("""
         CREATE EXTERNAL TABLE `%s_%s`(`a` int)
         PARTITIONED BY (`b` string)
         STORED AS PARQUET
         LOCATION '%s'
    """ % (table_name, version, uri))
    api.spark.sql("""
         CREATE VIEW `%s`
         AS SELECT * FROM `%s_%s`
    """ % (table_name, table_name, version))
    # make sure table doesn't exist
    rm(uri)
    # write
    api.write_table(
        df=api.spark.sql("SELECT 0 AS a, 'b'"),
        table_name=table_name,
        version=version,
        extra_write_config=lambda w: w.partitionBy("b"),
    )
    assert api.spark.sql("SELECT * FROM " + table_name).collect() == [
        Row(a=0, b="b"),
    ]
    # overwrite
    api.write_table(
        df=api.spark.sql("SELECT 1 AS a, 'b'"),
        table_name=table_name,
        version=version,
        extra_write_config=lambda w: w.mode("overwrite").partitionBy("b"),
    )
    assert api.spark.sql("SELECT * FROM " + table_name).collect() == [
        Row(a=1, b="b"),
    ]


def test_throw_value_error_on_missing_version(spark_fake):
    try:
        MozData(spark_fake).write_table(df=None, table_name="view")
    except ValueError as e:
        assert e.args[0] == "version required to write global table"


def test_throw_value_error_on_view(api):
    api.spark.sql("CREATE OR REPLACE TEMP VIEW view_v1 AS SELECT 0")
    try:
        api.write_table(
            df=None,
            table_name="view",
            version="v1",
            partition_values=[("a", "b")],
        )
    except ValueError as e:
        assert e.args[0] == "table is not external: view_v1"


def test_throw_value_error_on_invalid_metadata_update_method(api):
    try:
        api.write_table(
            df=api.spark.sql("SELECT 1"),
            table_name="invalid_metadata",
            version="v1",
            partition_values=[("a", "b")],
            extra_write_config=lambda w: w.format("csv"),
            metadata_update_methods=["method"],
        )
    except ValueError as e:
        assert e.args[0] == "Unknown metadata update method: method"
