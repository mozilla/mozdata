# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from .utils import (
    hadoop_ls,
    is_version,
    snake_case_to_camel,
    spark_list_tables,
)
from moztelemetry.dataset import Dataset
from os import environ
from uuid import uuid4

import boto3
import json
import logging
import pkg_resources
import requests

try:
    __version__ = pkg_resources.get_distribution("mozdata").version
except pkg_resources.DistributionNotFound:
    __version__ = "??"  # required to be something


def identity(value):
    return value


class MozData:
    """A consistent API for accessing Mozilla data that reports usage to Mozilla

    example:

    # create a custom dau by channel table
    api = MozData(spark)
    api.write_table(
      df=api.read_table("main_summary")
        .where("submission_date_s3='20180101'")
        .groupBy("submission_date_s3", "channel")
        .agg(countDistinct("client_id").as("dau"))
        .drop("submission_date_s3"),
      table_name="dau_by_channel",
      partition_values=[("submission_date_s3", "20180101")],
      owner="nobody@mozilla.com",
    )
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
            read_config=identity,
            write_config=identity,
            telemetry_url=None,
    ):
        """Instantiate an instance of the MozData api

        :param spark: SparkSession used to access data
        :param ad_hoc_tables_dir: optional location of ad hoc tables
        :param global_tables_dir: optional location of global tables
        :param default_metadata_update_methods: optional methods to use when
            updating table metadata
        :param read_config: optional function used to configure all
            DataFrameReaders
        :param write_config: optional function used to configure all
            DataFrameWriters
        :param telemetry_url: optional url where logs should be posted
        """
        self.spark = spark
        self.sc = spark.sparkContext  # for reading rdds
        self.jvm = spark._jvm  # for accessing hadoop
        self.ad_hoc_tables_dir = ad_hoc_tables_dir
        self.global_tables_dir = global_tables_dir
        self.default_metadata_update_methods = default_metadata_update_methods
        self.read_config = read_config
        self.write_config = write_config
        self.telemetry_url = telemetry_url

    def _log(self, action, **event):
        """Report an interaction with this api

        :param action: action being reported
        :type str:
        :param **event: fields describing the interaction
        :type Dict[str, str]:
        """
        event.update(apiVersion=__version__, apiCall=action)
        ping = json.dumps({
            snake_case_to_camel(k): v
            for k, v in event.items()
            if v is not None
        }, sort_keys=True)
        self.logger.debug(ping)
        if self.telemetry_url is not None:
            requests.post(
                self.telemetry_url + "/submit/mozdata/event/1/" + str(uuid4()),
                ping.encode(),
                headers={"content_type": "application/json"},
            )

    def list_rdds(self):
        """List the rdds available to readRDD

        example:

        # list raw dataset names
        api = MozData(spark)
        [source["name"] for source in api.list_rdds()]

        :return: list of source metadata objects, each updated with name of
            source
        """
        self._log("list_rdds")
        bucket = "net-mozaws-prod-us-west-2-pipeline-metadata"
        s3 = boto3.client("s3")
        raw = s3.get_object(Bucket=bucket, Key="sources.json")["Body"].read()
        sources = json.loads(raw)
        return [
            dict(name=name, **info)
            for name, info in sources.items()
        ]

    def list_tables(self, owner=None):
        """List the tables available to read_table

        example:

        api = MozData(spark)

        # list global tables
        api.list_tables()

        # list nobody@mozilla.com's tables
        api.list_tables(owner=Some("nobody@mozilla.com"))

        :param owner: optional email that identifies non-global namespace
        :return: list of table names
        """
        self._log(
            "list_tables",
            owner=owner,
            ad_hoc_tables_dir=owner and self.ad_hoc_tables_dir,
        )
        if owner is not None:
            tables_uri = self.ad_hoc_tables_dir + "/" + owner
            return [
                table for table in hadoop_ls(self.spark, tables_uri)[0]
                if any(
                    is_version(version)
                    for version in hadoop_ls(
                        self.spark,
                        tables_uri + "/" + table,
                    )[0]
                )
            ]
        else:
            return spark_list_tables(self.spark)

    def read_rdd(self, name, where=identity, **kwargs):
        """Read a raw dataset

        example:

        # read a little bit of raw telemetry
        api = MozData(spark)
        rdd = api.read_rdd(
            "telemetry",
            where=lambda d: d.where(sourceVersion="4"),
            limit=1,
        )

        :param name: dataset source name
        :param where: function to configure Dataset where clauses
        :param kwargs: passed to Dataset.records
        :return: RDD of Messages read
        """
        self._log("read_rdd", name=name)
        return where(Dataset.from_source(name)).records(sc=self.sc, **kwargs)

    def sql(self, query):
        """ Execute a SparkSQL query

        :param query: sql query to run
        :return: Dataframe of query results
        """
        self._log("sql", query=query)
        return self.spark.sql(query)
