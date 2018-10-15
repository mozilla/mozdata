# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

import re

location_in_create_table_statement_regex = re.compile(
    "(?s).*LOCATION[^']+'([^']+)'.*"
)


def hadoop_ls(spark, directory):
    """Use hadoop to list the contents of a directory

    :param directory: URI pointing to the desired directory
    :return: (dirnames, filenames) found
    """
    path = spark._jvm.org.apache.hadoop.fs.Path(directory)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        path.toUri(),
        spark.sparkContext._jsc.hadoopConfiguration(),
    )
    if fs.exists(path):
        listing = fs.listStatus(path)
        dir_names = [
            status.getPath().getName()
            for status in listing
            if status.isDirectory()
        ]
        file_names = [
            status.getPath().getName()
            for status in listing
            if status.isFile()
        ]
        return dir_names, file_names
    else:
        return [], []


def is_version(version):
    """Check that a string is in version format

    :param version: string that might be a version
    :return: true if version is in version format
    """
    return version[:1] == "v" and version[1:].isdigit()


def snake_case_to_camel(string):
    parts = string.split("_")
    head, tail = parts[0], parts[1:]
    return head + "".join(x.title() for x in tail)


def spark_list_tables(spark, table_name=None):
    """List tables available in spark sql

    :param spark: spark session used to access data
    :param table_name: optional specific table name to check for
    :return: list of table names
    """
    if table_name:
        table_name = "'%s'" % table_name
    return [
        t.tableName
        for t in (
            spark
            .sql("SHOW TABLES " + (table_name or ""))
            .collect()
        )
    ]
