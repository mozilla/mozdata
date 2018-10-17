# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from pyspark.sql import Row
import pytest
import os
import os.path
import shutil


def text_read_config(dataframe_reader):
    return dataframe_reader.format("text")


class Table(object):
    def __init__(self, table_name, owner, version, content, tables_dir):
        self.name = table_name
        self.owner = owner
        self.version = version
        self.content = content
        if owner is None:
            self.path = os.path.join(tables_dir, table_name, version)
        else:
            self.path = os.path.join(tables_dir, table_name, owner, version)
        os.makedirs(self.path)
        with open(os.path.join(self.path, "value.txt"), "w") as fp:
            fp.write(self.content)


@pytest.fixture(scope="module")
def ad_hoc_tables(api):
    tables = [
        Table(
            table_name="read_table",
            owner="read_table",
            version=version,
            content=version,
            tables_dir=api.ad_hoc_tables_dir
        )
        for version in ("v0", "v1")
    ]
    yield tables
    for table in tables:
        shutil.rmtree(table.path)


@pytest.fixture(scope="module")
def global_tables(api):
    tables = [
        Table(
            table_name="read_table",
            owner=None,
            version=version,
            content=version,
            tables_dir=api.global_tables_dir
        )
        for version in ("v2", "v3")
    ]
    yield tables
    for table in tables:
        shutil.rmtree(table.path)


def test_read_ad_hoc_tables(ad_hoc_tables, api):
    # test reading each table with version
    for table in ad_hoc_tables:
        assert api.read_table(
            table_name=table.name,
            version=table.version,
            extra_read_config=text_read_config,
            owner=table.owner,
        ).collect() == [Row(value=table.content)]
    # test determining latest as last table in list
    assert api.read_table(
        table_name=table.name,
        owner=table.owner,
        extra_read_config=text_read_config,
    ).collect() == [Row(value=table.content)]


def test_read_undefined_global_tables(api, global_tables):
    # test reading each table with version
    for table in global_tables:
        assert api.read_table(
            table_name=table.name,
            version=table.version,
            extra_read_config=text_read_config,
        ).collect() == [Row(value=table.content)]
    # test determining latest as last table in list
    assert api.read_table(
        table_name=table.name,
        extra_read_config=text_read_config,
    ).collect() == [Row(value=table.content)]


def test_read_defined_global_table_with_version(api, global_tables):
    table = global_tables[0]
    api.spark.sql("""
         CREATE EXTERNAL TABLE `%s_%s`(`text` string)
         STORED AS TEXTFILE
         LOCATION '%s'
    """ % (table.name, table.version, table.path))
    assert api.read_table(
        table_name=table.name,
        version=table.version,
    ).collect() == [Row(text=table.content)]


def test_read_defined_global_table_without_version(api, global_tables):
    table = global_tables[-1]
    api.spark.sql("""
         CREATE EXTERNAL TABLE `%s`(`text` string)
         STORED AS TEXTFILE
         LOCATION '%s'
    """ % (table.name, table.path))
    assert api.read_table(table.name).collect() == [Row(text=table.content)]
