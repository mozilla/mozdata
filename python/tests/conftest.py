# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from moto import mock_s3
from moztelemetry.heka.message_pb2 import Field, Header, Message
from pyspark.sql import SparkSession
import boto3
import mozdata
import os
import pytest
import shutil
import struct
import tempfile


class SparkFake:
    sparkContext = None
    _jvm = None

    def sql(self, *args, **kwargs):
        return args, kwargs


@pytest.fixture
def spark_fake():
    return SparkFake()


@pytest.fixture(scope="session")
def resources_dir():
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)


@pytest.fixture(scope="session")
def api(resources_dir):
    spark = (
        SparkSession
        .builder
        .master("local")
        .appName("python_mozdata_test")
        .config(  # hive metastore path
            "javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=%s;create=true" %
            os.path.join(resources_dir, "metastore")
        )
        .config(
            "spark.sql.warehouse.dir",
            os.path.join(resources_dir, "warehouse")
        )
        .enableHiveSupport()
        .getOrCreate()
    )
    yield mozdata.MozData(
        spark=spark,
        ad_hoc_tables_dir=os.path.join(resources_dir, "ad_hoc_tables"),
        global_tables_dir=os.path.join(resources_dir, "global_tables"),
    )
    spark.stop()


@pytest.fixture(scope="session")
def rdd():
    message = Message(
        uuid=b"1234",
        timestamp=0,
        fields=[
            Field(name="bytes", value_type=Field.BYTES, value_bytes=[b"foo"]),
            Field(name="string", value_type=Field.STRING,
                  value_string=["foo"]),
            Field(name="bool", value_type=Field.BOOL, value_bool=[True]),
            Field(name="double", value_type=Field.DOUBLE, value_double=[4.2]),
            Field(name="integer", value_type=Field.INTEGER,
                  value_integer=[42]),
            Field(name="string-with-int-value", value_type=Field.STRING,
                  value_string=["42"]),
            Field(name="submission", value_type=Field.STRING, value_string=["""
                {
                    "partiallyExtracted" : {
                        "alpha" : "1",
                        "beta" : "2"
                    },
                    "gamma": "3"
                }
            """]),
            Field(name="extracted.subfield", value_type=Field.STRING,
                  value_string=['{"delta": "4"}']),
            Field(name="extracted.nested.subfield", value_type=Field.STRING,
                  value_string=['{"epsilon": "5"}']),
            Field(name="partiallyExtracted.nested", value_type=Field.STRING,
                  value_string=['{"zeta": "6"}']),
        ]
    ).SerializeToString()
    header = Header(message_length=len(message)).SerializeToString()
    # "<B" means little-endian
    framed_message = (
        struct.pack("<B", 0x1E) +
        struct.pack("<B", len(header)) +
        header +
        struct.pack("<B", 0x1F) +
        message
    )
    bucket = "net-mozaws-prod-us-west-2-pipeline-metadata"
    with mock_s3():
        # create test resources
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="sources.json", Body="""
            {
                "test": {
                    "prefix": "test",
                    "metadata_prefix": "test",
                    "bucket": "%s"
                }
            }
        """ % bucket)
        s3.put_object(Bucket=bucket, Key="test/schema.json", Body="""
            {
                "dimensions": [
                    { "field_name": "key" }
                ]
            }
        """)
        heka_stream = framed_message * 42
        s3.put_object(Bucket=bucket, Key="test/val1/x", Body=heka_stream)
        s3.put_object(Bucket=bucket, Key="test/val2/x", Body=heka_stream)
        yield
        # clean up s3 resources
        for item in (
            s3
            .get_paginator("list_objects")
            .paginate(Bucket=bucket)
            .build_full_result()["Contents"]
        ):
            s3.delete_object(Bucket=bucket, Key=item["Key"])
        s3.delete_bucket(Bucket=bucket)
