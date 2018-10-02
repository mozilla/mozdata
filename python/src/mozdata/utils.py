# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

import re
from pyspark.sql.utils import AnalysisException

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
    return ''.join(x.title() for x in string.split("_"))


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


def sumdict(d1, d2):
    """Sum of two dicts

    :param d1: first dict
    :param d2: second dict
    :return: a new dict updated with the values of d1 and then d2
    """
    d3 = {}
    d3.update(d1)
    d3.update(d2)
    return d3


def uri_version(uri):
    """Try to get the table version from a uri

    :param uri: uri from which to attempt version extraction
    :return: version if found, or None
    """
    if uri is not None:
        version = uri.split("/").pop()
        if is_version(version):
            return version


class TableInfo:
    def __init__(self, table_name, version, owner, uri, spark,
                 ad_hoc_tables_dir, global_tables_dir):
        self.spark = spark
        self.table_name = table_name
        self.version = version
        self.owner = owner
        self.uri = uri
        self.in_catalog = False
        self.sql_table_name = table_name
        if version is not None:
            self.sql_table_name += "_" + version

        if uri is not None:
            self.version = uri_version(uri)

        elif owner is not None:
            self._fs_table("/".join([ad_hoc_tables_dir, owner, table_name]))

        else:
            if len(spark_list_tables(self.spark, self.sql_table_name)) < 1:
                # table is not present in catalog
                self._fs_table(global_tables_dir + "/" + table_name)

            else:
                # table is present in catalog
                self.in_catalog = True
                try:
                    match = location_in_create_table_statement_regex.match(
                        self.spark.sql(
                            "SHOW CREATE TABLE `%s`" % self.sql_table_name
                        ).collect()[0].createtab_stmt,
                    )
                    if match is None:
                        raise ValueError
                    self.uri = match.groups()[0]
                    self.version = uri_version(uri)
                except (AnalysisException, ValueError):
                    self.uri = None

    def _fs_table(self, table_uri):
        if self.version is None:
            versions = sorted(
                [
                    v
                    for v in hadoop_ls(self.spark, table_uri)[0]
                    if is_version(v)
                ],
                key=lambda x: int(x[1:])
            )
            if versions:
                self.version = versions.pop()
            else:
                self.version = "v1"
        self.uri = table_uri + "/" + self.version
