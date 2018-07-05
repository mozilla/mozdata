/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.mozdata

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse,equalToJson,post,postRequestedFor,stubFor,urlMatching,verify}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.Utils.createTempDir
import org.json4s.jackson.Serialization.writePretty
import org.json4s.{DefaultFormats,Formats,JArray,JField,JNothing,JObject,JValue}
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import io.findify.s3mock.S3Mock

class MozDataTest extends FlatSpec with Matchers with DataFrameSuiteBase {

  private val s3 = S3Mock(port = 8001)
  private val wireMockServer = new WireMockServer(wireMockConfig().port(9876))
  private val tempDir = createTempDir().toPath.toString
  private val adHocTablesDir = s"$tempDir/ad_hoc_tables"
  private val globalTablesDir = s"$tempDir/global_tables"
  lazy val api: MozData = MozData(
    spark=spark,
    adHocTablesDir=adHocTablesDir,
    globalTablesDir=globalTablesDir,
    readConfig={r=>r.format("json")},
    writeConfig={w=>w.format("json")},
    telemetryUrl=Some("http://localhost:9876")
  )

  override def beforeAll: Unit = {
    // start wire mock for telemetry
    wireMockServer.start()
    WireMock.configureFor("localhost", 9876)
    stubFor(post(urlMatching("^/submit/mozdata/event/1/([a-f0-9-]{36})$"))
      .willReturn(aResponse().withStatus(200)))
    // start s3Mock
    s3.start
    // send s3 requests to s3Mock for read_rdd and list_rdds
    DatasetTest.beforeAll()
    // set up spark
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    DatasetTest.afterAll()
    s3.shutdown
    WireMock.reset()
    wireMockServer.stop()
  }

  def verifyTelemetry(count: Int, action: String, event: Map[String,String] = Map()): Unit = {
    implicit val formats: Formats = DefaultFormats
    verify(count, postRequestedFor(
      urlMatching(s"/submit/mozdata/event/1/([a-f0-9-]{36})")
    ).withRequestBody(equalToJson(writePretty(Map("apiVersion" -> api.apiVersion, "apiCall" -> action)++event))))
  }

  "telemetry" must "not send when disabled" in {
    MozData(spark).sql("SELECT 1")
    verify(0, postRequestedFor(urlMatching(".*")))
  }

  "listRDDs" must "list rdds" in {
    api.listRDDs should be (Array(Map[String,String](
      "name" -> "test",
      "prefix" -> "test",
      "metadata_prefix" -> "test",
      "bucket" -> "net-mozaws-prod-us-west-2-pipeline-metadata"
    )))
    verifyTelemetry(1, "listRDDs", Map(
      "sourcesJson" -> s"s3://net-mozaws-prod-us-west-2-pipeline-metadata/sources.json"
    ))
  }

  "listTables" must "list global tables" in {
    // make sure tables don't exist
    api.listTables().sorted should be (List())
    // create temporary tables
    import spark.implicits._
    List(1).toDF.createOrReplaceTempView("table1")
    List(2).toDF.createOrReplaceTempView("table2")
    api.listTables().sorted should be (List("table1", "table2"))
    verifyTelemetry(2, "listTables", Map())
  }

  it must "list ad hoc tables" in {
    val owner = "listTables"
    // make sure tables don't exist
    Hadoop.rm(s"$adHocTablesDir/$owner")
    api.listTables(owner=Some(owner)).sorted should be (Array())
    // create tables
    import spark.implicits._
    api.writeTable(
      df=List[String]().toDF,
      tableName="table1",
      owner=Some(owner)
    )
    api.writeTable(
      df=List[String]().toDF,
      tableName="table2",
      owner=Some(owner)
    )
    api.listTables(owner=Some(owner))
      .sorted should be (Array("table1", "table2"))
    verifyTelemetry(2, "listTables", Map(
      "owner" -> owner,
      "adHocTablesDir" -> adHocTablesDir
    ))
  }

  // describeTable tests must be after listTables to avoid interference
  "describeTable" must "describe unversioned global table" in {
    val (tableName, version) = ("describe_table", "v1")
    val uri = s"$globalTablesDir/$tableName/$version"
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE `$tableName`(`0` int)
         |PARTITIONED BY (`1` string)
         |STORED AS TEXTFILE
         |LOCATION '$uri'
         |TBLPROPERTIES ('description'='$tableName $version')
      """.stripMargin)
    val meta = api.describeTable(tableName)
    meta("sqlTableName") should be (tableName)
    meta("detectedUri") should be (s"file:$uri")
    meta("detectedVersion") should be (version)
    meta("description") should be (s"$tableName $version")
    verifyTelemetry(1, "describeTable", Map(
      "detectedUri" -> s"file:$uri",
      "detectedVersion" -> version,
      "sqlTableName" -> tableName,
      "tableName"->tableName
    ))
  }

  it must "describe versioned view" in {
    val (tableName, version) = ("describe_table", "v2")
    spark.sql(s"CREATE OR REPLACE TEMP VIEW `${tableName}_$version` AS SELECT 0")
    val meta = api.describeTable(tableName, version=Some(version))
    meta("sqlTableName") should be (s"${tableName}_$version")
    meta.contains("detectedUri") should be (false)
    meta("detectedVersion") should be (version)
    verifyTelemetry(1, "describeTable", Map(
      "detectedVersion" -> version,
      "sqlTableName" -> s"${tableName}_$version",
      "tableName"->tableName,
      "version" -> version
    ))
  }

  it must "describe ad hoc table" in {
    val (owner, tableName, version) = ("describe_table", "describe_table", "v1")
    val uri = s"$adHocTablesDir/$owner/$tableName/$version"
    val meta = api.describeTable(tableName, owner=Some(owner))
    meta.contains("sqlTableName") should be (false)
    meta("detectedUri") should be (uri)
    meta("detectedVersion") should be (version)
    verifyTelemetry(1, "describeTable", Map(
      "detectedUri" -> uri,
      "detectedVersion" -> version,
      "owner" -> owner,
      "tableName"->tableName
    ))
  }

  it must "detect version in uri" in {
    val (owner, tableName, version) = ("uri", "describe_table", "v0")
    val uri = s"$adHocTablesDir/$owner/$tableName/$version"
    val meta = api.describeTable(tableName, uri=Some(uri))
    meta.contains("sqlTableName") should be (false)
    meta("detectedUri") should be (uri)
    meta("detectedVersion") should be (version)
    verifyTelemetry(1, "describeTable", Map(
      "detectedUri" -> uri,
      "detectedVersion" -> version,
      "tableName"->tableName
    ))
  }

  "readRDD" must "read rdd" in {
    def sortJValue(in: JValue): JValue = {
      in match {
        case JObject(fields) => JObject(fields
          .map{v=>JField(v._1,sortJValue(v._2))}
          .sortBy(_._1)
        )
        case JArray(elements) => JArray(elements.map{v=>sortJValue(v)})
        case _ => in
      }
    }
    val messages = api.readRDD(
      name="test"
    ).map{ message =>
      implicit val formats: Formats = DefaultFormats
      writePretty(sortJValue(message.toJValue.getOrElse(JNothing)))
    }
    messages.collect.foreach{ message =>
      message should be (
        """{
          |  "extracted" : {
          |    "nested" : {
          |      "subfield" : {
          |        "epsilon" : "5"
          |      }
          |    },
          |    "subfield" : {
          |      "delta" : "4"
          |    }
          |  },
          |  "gamma" : "3",
          |  "meta" : {
          |    "Timestamp" : 0,
          |    "bool" : true,
          |    "bytes" : "foo",
          |    "double" : 4.2,
          |    "integer" : 42,
          |    "string" : "foo",
          |    "string-with-int-value" : "42"
          |  },
          |  "partiallyExtracted" : {
          |    "alpha" : "1",
          |    "beta" : "2",
          |    "nested" : {
          |      "zeta" : "6"
          |    }
          |  }
          |}""".stripMargin
      )
    }
    messages.count should be (84)
    api.readRDD("test", where={w=>w.where("key"){case "val1" => true}}).count should be (42)
    api.readRDD("test", where={w=>w.where("key"){case "val2" => true}}).count should be (42)
    api.readRDD("test", fileLimit=Some(1)).count should be (42)
    api.readRDD("test", minPartitions=Some(2)).partitions.length should be (2)
    verifyTelemetry(5, "readRDD", Map("name" -> "test"))
  }

  "readTable" must "read latest ad hoc table" in {
    val (owner, tableName) = ("read_table", "read_table")
    var version = "v0"
    def uri: String = s"$adHocTablesDir/$owner/$tableName/$version"
    Hadoop.write(uri, version)
    version = "v1"
    Hadoop.write(uri, version)
    api.readTable(
      tableName=tableName,
      owner=Some(owner),
      extraReadConfig={r=>r.format("csv")}
    ).collect() should be (List(Row(Hadoop.read(uri))))
    verifyTelemetry(1, "readTable", Map(
      "detectedUri" -> uri,
      "detectedVersion" -> version,
      "owner" -> owner,
      "tableName" -> tableName
    ))
  }

  it must "read versioned ad hoc table" in {
    val (owner, tableName, version) = ("read_table", "read_table", "v0")
    val uri = s"$adHocTablesDir/$owner/$tableName/$version"
    Hadoop.write(uri, version)
    api.readTable(
      tableName=tableName,
      owner=Some(owner),
      version=Some(version),
      extraReadConfig={r=>r.format("csv")}
    ).collect() should be (List(Row(version)))
    verifyTelemetry(1, "readTable", Map(
      "detectedUri" -> uri,
      "detectedVersion" -> version,
      "owner" -> owner,
      "tableName" -> tableName,
      "version" -> version
    ))
  }

  it must "read latest undefined global table" in {
    val tableName = "read_table"
    var version = "v0"
    def uri: String = s"$globalTablesDir/$tableName/$version"
    Hadoop.write(uri, version)
    version = "v1"
    Hadoop.write(uri, version)
    api.readTable(
      tableName,
      extraReadConfig={r=>r.format("csv")}
    ).collect() should be (List(Row(version)))
    verifyTelemetry(1, "readTable", Map(
      "detectedUri" -> uri,
      "detectedVersion" -> version,
      "tableName" -> tableName
    ))
  }

  it must "read defined global table" in {
    val tableName = "read_table"
    spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW `$tableName` AS SELECT 1")
    api.readTable(tableName).collect() should be (List(Row(1)))
    verifyTelemetry(1, "readTable", Map(
      "tableName" -> tableName,
      "sqlTableName" -> tableName
    ))
  }

  "sql" must "run query" in {
    val query = "SELECT 0"
    api.sql(query).collect() should be (Array(Row(0)))
    verifyTelemetry(1, "sql", Map("query" -> query))
  }

  "writeTable" must "write ad_hoc table partitions" in {
    import spark.implicits._
    val (owner, tableName) = ("write_table_partition", "write_table_partition")
    var version = "v1"
    def uri: String = s"$adHocTablesDir/$owner/$tableName/$version"
    // make sure tables don't exist
    Hadoop.rm(uri.dropRight(3))
    // write new table
    api.writeTable(
      df=List("a").toDF("0"),
      tableName=tableName,
      partitionValues=List("1" -> "b", "2" -> "c"),
      owner=Some(owner)
    )
    spark.read.json(uri).collect() should be (List(Row("a", "b", "c")))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"$uri/1=b/2=c",
      "detectedVersion" -> version,
      "owner" -> owner,
      "partition" -> "1=b/2=c",
      "tableName" -> tableName
    ))
    // write new version
    version = "v2"
    api.writeTable(
      df=List(("d","f")).toDF("0", "2"),
      tableName=tableName,
      partitionValues=List("1" -> "e"),
      version=Some(version),
      owner=Some(owner),
      extraWriteConfig={w=>w.partitionBy("2")}
    )
    spark.read.json(uri).collect() should be (List(Row("d", "e", "f")))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"$uri/1=e",
      "detectedVersion" -> version,
      "owner" -> owner,
      "partition" -> "1=e",
      "tableName" -> tableName,
      "version" -> version
    ))
    // append to latest
    api.writeTable(
      df=List(("g","f")).toDF("0", "2"),
      tableName=tableName,
      partitionValues=List("1" -> "e"),
      owner=Some(owner),
      extraWriteConfig={w=>w.mode("append").partitionBy("2")}
    )
    // add new dynamic partition to latest
    api.writeTable(
      df=List(("h","j")).toDF("0", "2"),
      tableName=tableName,
      partitionValues=List("1" -> "i"),
      owner=Some(owner),
      extraWriteConfig={w=>w.partitionBy("2")}
    )
    // add new static partition to latest
    api.writeTable(
      df=List("k").toDF("0"),
      tableName=tableName,
      partitionValues=List("1" -> "l", "2" -> "m"),
      owner=Some(owner)
    )
    spark.read.json(uri).orderBy("0").collect() should be (List(
      Row("d","e","f"),
      Row("g","e","f"),
      Row("h","i","j"),
      Row("k","l","m")
    ))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"$uri/1=e",
      "detectedVersion" -> version,
      "owner" -> owner,
      "partition" -> "1=e",
      "tableName" -> tableName
    ))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"$uri/1=i",
      "detectedVersion" -> version,
      "owner" -> owner,
      "partition" -> "1=i",
      "tableName" -> tableName
    ))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"$uri/1=l/2=m",
      "detectedVersion" -> version,
      "owner" -> owner,
      "partition" -> "1=l/2=m",
      "tableName" -> tableName
    ))
  }

  it must "write partition uris" in {
    import spark.implicits._
    val tableName = "write_table_partition"
    val uri = s"$adHocTablesDir/uri/$tableName"
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write only uri
    api.writeTable(
      df=List("a").toDF("0"),
      tableName=tableName,
      uri=Some(s"$uri/1=b/2=c")
    )
    // write mixed uri & partition spec
    api.writeTable(
      df=List("d").toDF("0"),
      tableName=tableName,
      partitionValues=List("2" -> "f"),
      uri=Some(s"$uri/1=e")
    )
    // write all partitions in partition spec
    api.writeTable(
      df=List("g").toDF("0"),
      tableName=tableName,
      partitionValues=List("1" -> "h", "2" -> "i"),
      uri=Some(uri)
    )
    spark.read.json(uri).orderBy("0").collect() should be (List(
      Row("a","b","c"),
      Row("d","e","f"),
      Row("g","h","i")))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"$uri/1=b/2=c",
      "tableName" -> tableName,
      "uri" -> s"$uri/1=b/2=c"
    ))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"$uri/1=e/2=f",
      "partition" -> "2=f",
      "tableName" -> tableName,
      "uri" -> s"$uri/1=e"
    ))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"$uri/1=h/2=i",
      "partition" -> "1=h/2=i",
      "tableName" -> tableName,
      "uri" -> uri
    ))
  }

  it must "write undefined global table partition" in {
    import spark.implicits._
    val (tableName, version) = ("write_table_partition", "v1")
    val uri = s"$globalTablesDir/$tableName/$version"
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write
    api.writeTable(
      df=List("a").toDF,
      tableName=tableName,
      partitionValues=List("b" -> "b"),
      version=Some(version)
    )
    spark.read.json(uri).collect() should be (List(Row("a", "b")))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"$uri/b=b",
      "detectedVersion" -> version,
      "partition" -> "b=b",
      "tableName" -> tableName,
      "version" -> version
    ))
  }

  it must "write and overwrite defined global table partitions" in {
    import spark.implicits._
    val (tableName, version) = ("write_table_partition", "v3")
    val uri = s"$globalTablesDir/$tableName/$version"
    // create table in catalog
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE `${tableName}_$version`(`0` int)
         |PARTITIONED BY (`1` string, `2` string)
         |STORED AS TEXTFILE
         |LOCATION '$uri'
      """.stripMargin)
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write
    api.writeTable(
      df=List(0).toDF("0"),
      tableName=tableName,
      partitionValues=List("1" -> "a", "2" -> "b"),
      version=Some(version),
      extraWriteConfig={w=>w.format("csv")}
    )
    spark.sql(s"SELECT * FROM `${tableName}_$version`").collect() should be (List(Row(0, "a", "b")))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"file:$uri/1=a/2=b",
      "detectedVersion" -> version,
      "partition" -> "1=a/2=b",
      "sqlTableName" -> s"${tableName}_$version",
      "tableName" -> tableName,
      "version" -> version
    ))
    // overwrite
    api.writeTable(
      df=List((1,"b")).toDF("0", "2"),
      tableName=tableName,
      partitionValues=List("1" -> "a"),
      version=Some(version),
      extraWriteConfig={w=>w.format("csv").mode("overwrite").partitionBy("2")}
    )
    spark.sql(s"SELECT * FROM `${tableName}_$version`").collect() should be (List(Row(1, "a", "b")))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"file:$uri/1=a",
      "detectedVersion" -> version,
      "partition" -> "1=a",
      "tableName" -> tableName,
      "sqlTableName" -> s"${tableName}_$version",
      "version" -> version
    ))
  }

  it must "write ad_hoc tables" in {
    import spark.implicits._
    val (owner, tableName) = ("write_table", "write_table")
    var version = "v1"
    def uri: String = s"$adHocTablesDir/$owner/$tableName/$version"
    // make sure tables don't exist
    Hadoop.rm(uri.dropRight(3))
    // write new table
    api.writeTable(
      df=List(("a", "b")).toDF,
      tableName=tableName,
      owner=Some(owner)
    )
    spark.read.json(uri).collect() should be (Array(Row("a", "b")))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> uri,
      "detectedVersion" -> version,
      "owner" -> owner,
      "tableName" -> tableName
    ))
    // write new version
    version = "v2"
    api.writeTable(
      df=List(("c", "d")).toDF,
      tableName=tableName,
      owner=Some(owner),
      version=Some(version)
    )
    spark.read.json(uri).collect() should be (Array(Row("c", "d")))
    // append to latest
    api.writeTable(
      df=List(("e", "f")).toDF,
      tableName=tableName,
      owner=Some(owner),
      extraWriteConfig={w=>w.mode("append")}
    )
    spark.read.json(uri).orderBy("_1").collect() should be (List(Row("c","d"),Row("e","f")))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> uri,
      "detectedVersion" -> version,
      "owner" -> owner,
      "tableName" -> tableName,
      "version" -> version
    ))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> uri,
      "detectedVersion" -> version,
      "owner" -> owner,
      "tableName" -> tableName
    ))
  }

  it must "write table uri" in {
    import spark.implicits._
    val (tableName, version) = ("write_table", "v1")
    val uri = s"$adHocTablesDir/uri/$tableName/$version"
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write
    api.writeTable(
      df=List(("a", "b")).toDF,
      tableName=tableName,
      uri=Some(uri)
    )
    spark.read.json(uri).collect() should be (Array(Row("a", "b")))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> uri,
      "detectedVersion" -> version,
      "tableName" -> tableName,
      "uri" -> uri
    ))
  }

  it must "write undefined global table" in {
    import spark.implicits._
    val (tableName, version) = ("write_table", "v1")
    val uri = s"$globalTablesDir/$tableName/$version"
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write
    api.writeTable(
      df=List(("a", "b")).toDF,
      tableName=tableName,
      version=Some(version)
    )
    spark.read.json(uri).collect() should be (Array(Row("a", "b")))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> uri,
      "detectedVersion" -> version,
      "tableName" -> tableName,
      "version" -> version
    ))
  }

  it must "write defined global table" in {
    import spark.implicits._
    val (tableName, version) = ("write_table", "v2")
    val uri = s"$globalTablesDir/$tableName/$version"
    // create table in catalog
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE `${tableName}_$version`(`0` int)
         |STORED AS TEXTFILE
         |LOCATION '$uri'
      """.stripMargin)
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write
    api.writeTable(
      df=List(0).toDF("0"),
      tableName=tableName,
      version=Some(version),
      // must disable "sql_repair" update for unpartitioned table
      metadataUpdateMethods=List("sql_refresh"),
      extraWriteConfig={w=>w.format("csv")}
    )
    spark.sql(s"SELECT * FROM `${tableName}_$version`").collect() should be (List(Row(0)))
    verifyTelemetry(1, "writeTable", Map(
      "detectedUri" -> s"file:$uri",
      "detectedVersion" -> version,
      "sqlTableName" -> s"${tableName}_$version",
      "tableName" -> tableName,
      "version" -> version
    ))
  }

  it must "write and overwrite defined partitioned global table" in {
    import spark.implicits._
    val (tableName, version) = ("write_table", "v3")
    val uri = s"$globalTablesDir/$tableName/$version"
    // create table in catalog
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE `${tableName}_$version`(`0` int)
         |PARTITIONED BY (`1` string)
         |STORED AS TEXTFILE
         |LOCATION '$uri'
      """.stripMargin)
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write
    api.writeTable(
      df=List((0, "a")).toDF("0", "1"),
      tableName=tableName,
      version=Some(version),
      extraWriteConfig={w=>w.format("csv").partitionBy("1")}
    )
    spark.sql(s"SELECT * FROM `${tableName}_$version`").collect() should be (List(Row(0, "a")))
    // NOTE: metadata update is currently unable to drop partitions
    //       and partitions deleted by rewrite will cause read failures.
    // overwrite
    api.writeTable(
      df=List((1, "a")).toDF("0", "1"),
      tableName=tableName,
      version=Some(version),
      extraWriteConfig={w=>w.format("csv").mode("overwrite").partitionBy("1")}
    )
    spark.sql(s"SELECT * FROM `${tableName}_$version`").collect() should be (List(Row(1, "a")))
    verifyTelemetry(2, "writeTable", Map(
      "detectedUri" -> s"file:$uri",
      "detectedVersion" -> version,
      "sqlTableName" -> s"${tableName}_$version",
      "tableName" -> tableName,
      "version" -> version
    ))
  }

  it must "throw an exception on missing version" in {
    val thrown = intercept[IllegalArgumentException] {
      api.writeTable(
        df=spark.sql("SELECT 1"),
        tableName="view"
      )
    }
    assert(thrown.getMessage === "requirement failed: version required to write global table")
  }

  it must "throw an exception on internal table" in {
    spark.sql("CREATE TABLE table_v1 AS SELECT 0")
    val thrown = intercept[IllegalArgumentException] {
      api.writeTable(
        df=spark.sql("SELECT 1"),
        tableName="table",
        version=Some("v1")
      )
    }
    assert(thrown.getMessage === "requirement failed: table is not external: table_v1")
  }

  it must "throw an exception on view" in {
    spark.sql("CREATE OR REPLACE TEMP VIEW view_v1 AS SELECT 0")
    val thrown = intercept[IllegalArgumentException] {
      api.writeTable(
        df=spark.sql("SELECT 1"),
        tableName="view",
        version=Some("v1"),
        partitionValues=List(("a","b"))
      )
    }
    assert(thrown.getMessage === "requirement failed: table is not external: view_v1")
  }

  it must "throw an exception on invalid metadata update method" in {
    spark.sql(
      s"""CREATE EXTERNAL TABLE invalid_metadata_v1(`1` int)
         |PARTITIONED BY (a string)
         |STORED AS TEXTFILE
         |LOCATION '$globalTablesDir/invalid_metdata/v1'
         |""".stripMargin
    )
    val thrown = intercept[IllegalArgumentException] {
      api.writeTable(
        df=spark.sql("SELECT 1"),
        tableName="invalid_metadata",
        version=Some("v1"),
        partitionValues=List(("a", "b")),
        extraWriteConfig={w=>w.format("csv")},
        metadataUpdateMethods=List("method")
      )
    }
    assert(thrown.getMessage === "Unsupported metadata location: method")
  }
}
