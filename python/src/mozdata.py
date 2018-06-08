# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

import boto3
import json
from moztelemetry.dataset import Dataset
from py4j.java_collections import ListConverter
from pyspark.sql import DataFrame

class MozData():
    """A consistent API for accessing Mozilla data that reports usage to Mozilla
    """
    def __init__(self, spark, ad_hoc_tables_dir=None, global_tables_dir=None,
                 default_metadata_update_methods=None):
        self.spark = spark
        self.api = spark._jvm.com.mozilla.telemetry.mozdata.PyMozData(
            spark._jsparkSession,
            ad_hoc_tables_dir,
            global_tables_dir,
            default_metadata_update_methods,
        )

    def _sumdict(self, d1, d2):
        d3 = {}
        d3.update(d1)
        d3.update(d2)
        return d3

    def list_rdds(self):
        self.api.logListRDDs()
        bucket = "net-mozaws-prod-us-west-2-pipeline-metadata"
        raw = boto3.client("s3").get_object(
            Bucket=bucket,
            Key="sources.json",
        )["Body"].read()
        sources = json.loads(raw)
        return [
            self._sumdict(info, {"name":name})
            for name, info in sources.items()
        ]

    def list_tables(self, owner=None):
        jlist = self.api.listTables(owner)
        # convert to python type and return
        return ListConverter().convert(jlist, jlist._gateway_client)

    def read_rdd(self, name, where=lambda x:x, **kwargs):
        self.api.logReadRDD(name)
        return where(Dataset.from_source(name)).records(
            sc=self.spark.sparkContext,
            **kwargs
        )

    def read_table(self, table_name, version=None, owner=None, uri=None,
                   read_config=None):
        # call out to java api
        jdf = self.api.readTable(
            table_name,
            version,
            owner,
            uri,
            read_config,
        )
        # convert to python type and return
        return DataFrame(jdf, self.spark)

    def sql(self, query):
        # convert to python type and return
        return DataFrame(self.api.sql(query), self.spark)

    def write_partition(self, df, table_name, partition=None, version=None,
                        owner=None, uri=None, metadata_update_methods=None,
                        write_config=None):
        if write_config is not None:
            write_config = {  # automatically convert string values to lists
                key: value if type(value) == list else [value]
                for key, value in write_config.items()
            }
        self.api.writePartition(
            df._jdf,
            table_name,
            partition,
            version,
            owner,
            uri,
            metadata_update_methods,
            write_config,
        )

    def write_table(self, df, table_name, version=None, owner=None, uri=None,
                    metadata_update_methods=None, write_config=None):
        if write_config is not None:
            write_config = {  # automatically convert string values to lists
                key: value if type(value) == list else [value]
                for key, value in write_config.items()
            }
        self.api.writeTable(
            df._jdf,
            table_name,
            version,
            owner,
            uri,
            metadata_update_methods,
            write_config,
        )
