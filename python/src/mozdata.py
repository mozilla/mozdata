# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

import boto3
import json
import logging
import re
from os import environ
from moztelemetry.dataset import Dataset
from pyspark.sql.utils import AnalysisException
import pkg_resources

__version__ = pkg_resources.get_distribution("mozdata").version


class MozData:
    """A consistent API for accessing Mozilla data that reports usage to Mozilla
    """

    logger = logging.getLogger("MozData")

    def __init__(
            self,
            spark,
            ad_hoc_tables_dir=environ.get(
                "AD_HOC_TABLES_DIR",
                "s3://net-mozaws-prod-us-west-2-pipeline-analysis",
            ),
            global_tables_dir=environ.get(
                "GLOBAL_TABLES_DIR",
                "s3://telemetry-parquet",
            ),
            default_metadata_update_methods=environ.get(
                "DEFAULT_METADATA_UPDATE_METHODS",
                "sql_repair,sql_refresh",
            ).split(","),
    ):
        """Instantiate an instance of the MozData api

        :param spark: SparkSession used to access data
        :param ad_hoc_tables_dir: optional location of ad hoc tables
        :param global_tables_dir: optional location of global tables
        :param default_metadata_update_methods: optional methods to use when
         updating table metadata
        """
        self.spark = spark
        self.sc = spark.sparkContext  # for reading rdds
        self.jvm = spark._jvm  # for accessing hadoop
        self.ad_hoc_tables_dir = ad_hoc_tables_dir
        self.global_tables_dir = global_tables_dir
        self.default_metadata_update_methods = default_metadata_update_methods

    @staticmethod
    def _sumdict(d1, d2):
        """Sum of two dicts

        :param d1: first dict
        :param d2: second dict
        :return: a new dict updated with the values of d1 and then d2
        """
        d3 = {}
        d3.update(d1)
        d3.update(d2)
        return d3

    @staticmethod
    def _is_version(version):
        """Check that a string is in version format

        :param version: string that might be a version
        :return: true if version is in version format
        """
        return version[:1] == "v" and version[1:].isdigit()

    def _log(self, action, metadata=None):
        """Report an interaction with this api

        :param action: action being reported
        :param metadata: dict of fields describing the interaction
        """
        ping = json.dumps(self._sumdict({
            k: v
            for k, v in (metadata or {}).items()
            if v is not None
            },
            {"api_version": __version__}
        ))
        self.logger.debug(": ".join([action, ping]))

    def _update_metadata(self, sql_table_name, method):
        """Update the metadata for a table

        :param sql_table_name: table to update
        :param method: method to use to update the table
        """
        if method == "sql_repair":
            self.spark.sql("MSCK REPAIR TABLE `%s`" % sql_table_name)
        elif method == "sql_refresh":
            self.spark.sql("REFRESH TABLE `%s`" % sql_table_name)
        else:
            raise ValueError("Unknown metadata update method: " + method)

    def _uri_version(self, uri):
        """Try to get the table version from a uri

        :param uri: uri from which to attempt version extraction
        :return: version if found, or None
        """
        if uri is not None:
            version = uri.split("/").pop()
            if self._is_version(version):
                return version

    def _hadoop_ls(self, directory):
        """Use hadoop to list the contents of a directory

        :param directory: URI pointing to the desired directory
        :return: (dirnames, filenames) found
        """
        path = self.jvm.org.apache.hadoop.fs.Path(directory)
        fs = self.jvm.org.apache.hadoop.fs.FileSystem.get(
            path.toUri(),
            self.sc._jsc.hadoopConfiguration(),
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

    def _get_fs_table(self, table_uri, version):
        """Extract non-catalog table info

        reusable code for _get_table_info

        :param table_uri: uri of table without version
        :param version: optional specific version of table
        :return: uri of table with version, detected table version, None
        """
        if version is not None:
            detected_version = version
        else:
            versions = sorted(
                [v for v in self._hadoop_ls(table_uri)[0] if v[1:].isdigit()],
                key=lambda x: int(x[1:])
            )
            if versions:
                detected_version = versions.pop()
            else:
                detected_version = "v1"
        return "/".join([table_uri, detected_version]), detected_version, None

    def _get_table_info(self, table_name, version, owner, uri):
        """Locate information needed for accessing a table

        :param table_name: table to look up
        :param version: optional specific version of table
        :param owner: optional email that identifies non-global namespace
        :param uri: optional non-standard location for this table
        :return: (detected_uri, detected_version, sql_table_name) of table, at
            least one of detected_uri or sql_table_name will be defined
        """
        if uri is not None:
            return uri, self._uri_version(uri), None
        elif owner is not None:
            return self._get_fs_table(
                "/".join([self.ad_hoc_tables_dir, owner, table_name]),
                version,
            )
        else:
            sql_table_name = table_name
            if version is not None:
                sql_table_name += "_" + version
            if len(self.spark.sql("SHOW TABLES '%s'" % sql_table_name).collect()) < 1:
                # table does not exist in catalog
                return self._get_fs_table(
                    "/".join([self.global_tables_dir, table_name]),
                    version,
                )
            else:
                try:
                    match = re.match(
                        "(?s).*LOCATION[^']+'([^']+)'.*",
                        self.spark.sql(
                            "SHOW CREATE TABLE `%s`" % sql_table_name
                        ).collect()[0].createtab_stmt,
                    )
                    if match is None:
                        raise ValueError
                    location = match.groups()[0]
                    return location, self._uri_version(location), sql_table_name
                except (AnalysisException, ValueError):
                    return None, version, sql_table_name

    def list_rdds(self):
        """ List the sources available to read_rdd

        :return: list of source metadata objects, each updated with name of source
        """
        self._log("list_rdds")
        bucket = "net-mozaws-prod-us-west-2-pipeline-metadata"
        raw = boto3.client("s3").get_object(Bucket=bucket, Key="sources.json")["Body"].read()
        sources = json.loads(raw)
        return [self._sumdict(info, {"name": name}) for name, info in sources.items()]

    def list_tables(self, owner=None):
        """List the tables available to read_table

        :param owner: optional email that identifies non-global namespace
        :return: list of table names
        """
        if owner is None:
            self._log("list_tables")
            return [t.tableName for t in self.spark.sql("SHOW TABLES").collect()]
        else:
            self._log("list_tables", dict(
                owner=owner,
                ad_hoc_tables_dir=self.ad_hoc_tables_dir,
            ))
            tables_uri = "/".join([self.ad_hoc_tables_dir, owner])
            return [
                table
                for table in self._hadoop_ls(tables_uri)[0]
                if any(
                    self._is_version(version)
                    for version in self._hadoop_ls("/".join([tables_uri, table]))[0]
                )
            ]

    def read_rdd(self, name, where=lambda x: x, **kwargs):
        """Read a raw dataset

        :param name: dataset source name
        :param where: function to configure Dataset where clauses
        :param kwargs: passed to Dataset.records
        :return: RDD of Messages read
        """
        self._log("read_rdd", dict(name=name))
        return where(Dataset.from_source(name)).records(sc=self.sc, **kwargs)

    def read_table(self, table_name, version=None, owner=None, uri=None,
                   read_config=lambda x: x):
        """Read a table

        :param table_name: table to read
        :param version: optional specific version of table, defaults to "v1" for
            new tables and the latest version for existing tables
        :param owner: optional email that identifies non-global namespace
        :param uri: optional non-standard location for this table
        :param read_config: optional function to configure the DataFrameReader
        :return: Dataframe of the requested table
        """
        detected_uri, detected_version, sql_table_name = self._get_table_info(
            table_name=table_name,
            version=version,
            owner=owner,
            uri=uri,
        )
        self._log("read_table", dict(
            table_name=table_name,
            version=version,
            owner=owner,
            uri=uri,
        ))
        # return read table
        if sql_table_name is not None:
            return read_config(self.spark.read).table(sql_table_name)
        else:
            return read_config(self.spark.read).load(detected_uri)

    def sql(self, query):
        """ Execute a SparkSQL query

        :param query: sql query to run
        :return: Dataframe of query results
        """
        self._log("sql", dict(query=query))
        return self.spark.sql(query)

    def write_partition(self, df, table_name, partition=None, version=None,
                        owner=None, uri=None, metadata_update_methods=None,
                        write_config=lambda x: x):
        """Write partition to long term storage

        :param df: DataFrame to write
        :param table_name: table to write
        :param partition: ordered list of key-value static partition identifiers,
            which must be absent from df, optional if specifying uri
        :param version: specific version of table, required for global tables,
            defaults to latest or "v1" if latest can't be determined
        :param owner: optional email that identifies non-global namespace
        :param uri: optional non-standard location for this table
        :param metadata_update_methods: optional override of methods to use to
            update metadata after writing partitioned global tables
        :param write_config: optional function to configure the DataFrameWriter
        """
        if metadata_update_methods is None:
            metadata_update_methods = self.default_metadata_update_methods
        detected_table_uri, detected_version, sql_table_name = self._get_table_info(
            table_name=table_name,
            version=version,
            owner=owner,
            uri=uri,
        )
        partition_string = "/".join(
            "=".join([k, v])
            for k, v in partition or []
        ) or None
        detected_uri = "/".join(
            item
            for item in (uri or detected_table_uri, partition_string)
            if item is not None
        )
        self._log("write_partition", dict(
            table_name=table_name,
            partition=partition_string,
            version=version,
            owner=owner,
            uri=uri,
            metadata_update_methods=",".join(metadata_update_methods),
        ))
        # write partition
        write_config(df.write).save(detected_uri)
        if sql_table_name is not None:
            map(
                lambda m: self._update_metadata(sql_table_name, m),
                metadata_update_methods,
            )

    def write_table(self, df, table_name, version=None, owner=None, uri=None,
                    metadata_update_methods=None, write_config=lambda x: x):
        """Write table to long term storage

        :param df: DataFrame to write
        :param table_name: table to write
        :param version: specific version of table, required for global tables,
            defaults to latest or "v1" if latest can't be determined
        :param owner: optional email that identifies non-global namespace
        :param uri: optional non-standard location for this table
        :param metadata_update_methods: optional override of methods to use to
            update metadata after writing partitioned global tables
        :param write_config: optional function to configure the DataFrameWriter
        """
        if metadata_update_methods is None:
            metadata_update_methods = self.default_metadata_update_methods
        detected_uri, detected_version, sql_table_name = self._get_table_info(
            table_name=table_name,
            version=version,
            owner=owner,
            uri=uri,
        )
        self._log("write_table", dict(
            table_name=table_name,
            version=version,
            owner=owner,
            uri=uri,
            metadata_update_methods=",".join(metadata_update_methods),
        ))
        # write table
        write_config(df.write).save(detected_uri)
        if sql_table_name is not None:
            map(
                lambda m: self._update_metadata(sql_table_name, m),
                metadata_update_methods,
            )
