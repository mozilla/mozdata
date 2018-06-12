/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.mozdata

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.json4s.jackson.Serialization.writePretty
import org.json4s._
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

import scala.util.control.Breaks.break

class MozDataTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  private val adHocTablesDir = getClass.getResource("/ad_hoc_tables").toString
  private val globalTablesDir = getClass.getResource("/global_tables").toString
  lazy val api: MozData = MozData(
    spark=spark,
    adHocTablesDir=adHocTablesDir,
    globalTablesDir=globalTablesDir
  )
  var moto: java.lang.Process = _

  override def beforeAll: Unit = {
    val port = 8001
    moto = Runtime.getRuntime.exec(s"moto_server s3 -p $port")
    for (i <- 1 to 60) { // wait up to 60 seconds for moto
      try {
        val socket = new java.net.Socket("127.0.0.1", port)
        socket.close()
        break
      } catch {
        case _: Throwable => if (moto.isAlive) {Thread.sleep(1000)} // wait for moto to initialize
      }
    }
    DatasetTest.beforeAll()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    DatasetTest.afterAll()
    moto.destroyForcibly
  }

  "listRDDs" must "list rdds" in {
    api.listRDDs should be (Array(Map[String,String](
      "name"->"test",
      "prefix"->"test",
      "metadata_prefix"->"test",
      "bucket"->"net-mozaws-prod-us-west-2-pipeline-metadata"
    )))
  }

  "listTables" must "list global tables" in {
    // make sure tables don't exist
    api.listTables().sorted should be (List())
    // create temporary tables
    import spark.implicits._
    List(1).toDF.createOrReplaceTempView("table1")
    List(2).toDF.createOrReplaceTempView("table2")
    api.listTables().sorted should be (List("table1", "table2"))
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
  }

  "readTable" must "read latest ad hoc table" in {
    api.readTable(
      tableName="read_table",
      owner=Some("read_table"),
      readConfig = {r=>r.format("csv")}
    ).collect() should be(List(Row("1")))
  }

  it must "read versioned ad hoc table" in {
    api.readTable(
      tableName="read_table",
      owner=Some("read_table"),
      version=Some("v0"),
      readConfig = {r=>r.format("csv")}
    ).collect() should be(List(Row("0")))
  }

  it must "read latest undefined global table" in {
    val tableName = "read_table"
    api.readTable(
      tableName,
      readConfig = {r=>r.format("csv")}
    ).collect() should be(List(Row("1")))
  }

  it must "read latest defined global table" in {
    val tableName = "read_table"
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE `$tableName`(`0` int)
         |STORED AS TEXTFILE
         |LOCATION '$globalTablesDir/$tableName/v1'
      """.stripMargin)
    api.readTable(tableName).collect() should be(List(Row(1)))
  }

  "sql" must "run query" in {
    api.sql(s"SELECT 0").collect() should be (Array(Row(0)))
  }

  "writeTable" must "write ad_hoc tables" in {
    import spark.implicits._
    val (owner, tableName) = ("write_table", "write_table")
    // make sure tables don't exist
    Hadoop.rm(s"$adHocTablesDir/$owner/$tableName")
    // write new table
    api.writeTable(
      df=List(("a", "b")).toDF,
      tableName=tableName,
      owner=Some(owner)
    )
    api.readTable(tableName, owner=Some(owner))
      .collect() should be (Array(Row("a", "b")))
    // write new version
    api.writeTable(
      df=List(("c", "d")).toDF,
      tableName=tableName,
      owner=Some(owner),
      version=Some("v2")
    )
    api.readTable(tableName, owner=Some(owner))
      .collect() should be (Array(Row("c", "d")))
    // append to latest
    api.writeTable(
      df=List(("e", "f")).toDF,
      tableName=tableName,
      owner=Some(owner),
      writeConfig={w=>w.mode("append")}
    )
    api.readTable(tableName, owner=Some(owner))
      .orderBy("_1")
      .collect() should be (List(Row("c","d"),Row("e","f")))
  }

  it must "write table uri" in {
    import spark.implicits._
    val tableName = "write_table"
    val uri = s"$adHocTablesDir/uri/$tableName"
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write
    api.writeTable(
      df=List(("a", "b")).toDF,
      tableName=tableName,
      uri=Some(uri)
    )
    api.readTable(tableName, uri=Some(uri))
      .collect() should be (Array(Row("a", "b")))
  }

  it must "write undefined global table" in {
    import spark.implicits._
    val (tableName, version) = ("write_table", "v1")
    // make sure table doesn't exist
    Hadoop.rm(s"$globalTablesDir/$tableName")
    // write
    api.writeTable(
      df=List(("a", "b")).toDF,
      tableName=tableName,
      version=Some(version)
    )
    api.readTable(tableName, Some(version))
      .collect() should be (Array(Row("a", "b")))
    api.readTable(tableName)
      .collect() should be (Array(Row("a", "b")))
  }

  it must "write defined global table" in {
    import spark.implicits._
    val (tableName, version) = ("write_table", "v2")
    val uri = s"$globalTablesDir/$tableName/$version"
    // create table in catalog
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE `${tableName}_$version`(`0` int)
         |STORED AS PARQUET
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
      metadataUpdateMethods = Some(List("sql_refresh"))
    )
    api.readTable(tableName, Some(version))
      .collect() should be (List(Row(0)))
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
         |STORED AS PARQUET
         |LOCATION '$uri'
      """.stripMargin)
    // create unversioned table as view
    spark.sql(
      s"""
         |CREATE VIEW `$tableName`
         |AS SELECT * FROM `${tableName}_$version`
      """.stripMargin)
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write
    api.writeTable(
      df=List((0, "a")).toDF("0", "1"),
      tableName=tableName,
      version=Some(version),
      writeConfig={w=>w.partitionBy("1")}
    )
    api.readTable(tableName).collect() should be (List(Row(0, "a")))
    // NOTE: metadata update is currently unable to drop partitions
    //       and partitions deleted by rewrite will cause read failures.
    // overwrite
    api.writeTable(
      df=List((1, "a")).toDF("0", "1"),
      tableName=tableName,
      version=Some(version),
      writeConfig={w=>w.mode("overwrite").partitionBy("1")}
    )
    api.readTable(tableName).collect() should be (List(Row(1, "a")))
  }

  "writePartition" must "write ad_hoc table partitions" in {
    import spark.implicits._
    val (owner, tableName) = ("write_partition", "write_partition")
    // make sure tables don't exist
    Hadoop.rm(s"$adHocTablesDir/$owner/$tableName")
    // write new table
    api.writePartition(
      df=List("a").toDF("0"),
      tableName=tableName,
      partition=List("1"->"b", "2"->"c"),
      owner=Some(owner)
    )
    api.readTable(tableName, owner=Some(owner))
      .collect() should be (List(Row("a", "b", "c")))
    // write new version
    api.writePartition(
      df=List(("d","f")).toDF("0", "2"),
      tableName=tableName,
      partition=List("1"->"e"),
      version=Some("v2"),
      owner=Some(owner),
      writeConfig={w=>w.partitionBy("2")}
    )
    api.readTable(tableName, owner=Some(owner))
      .collect() should be (List(Row("d", "e", "f")))
    // append to latest
    api.writePartition(
      df=List(("g","f")).toDF("0", "2"),
      tableName=tableName,
      partition=List("1"->"e"),
      owner=Some(owner),
      writeConfig={w=>w.mode("append").partitionBy("2")}
    )
    // add new dynamic partition to latest
    api.writePartition(
      df=List(("h","j")).toDF("0", "2"),
      tableName=tableName,
      partition=List("1"->"i"),
      owner=Some(owner),
      writeConfig={w=>w.partitionBy("2")}
    )
    // add new static partition to latest
    api.writePartition(
      df=List("k").toDF("0"),
      tableName=tableName,
      partition=List("1"->"l", "2"->"m"),
      owner=Some(owner)
    )
    api.readTable(tableName, owner=Some(owner))
      .orderBy("0")
      .collect() should be (List(
        Row("d","e","f"),
        Row("g","e","f"),
        Row("h","i","j"),
        Row("k","l","m")
    ))
  }

  it must "write partition uris" in {
    import spark.implicits._
    val tableName = "write_partition"
    val uri = s"$adHocTablesDir/uri/$tableName"
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write only uri
    api.writePartition(
      df=List("a").toDF("0"),
      tableName=tableName,
      uri=Some(s"$uri/1=b/2=c")
    )
    // write mixed uri & partition spec
    api.writePartition(
      df=List("d").toDF("0"),
      tableName=tableName,
      partition=List("2"->"f"),
      uri=Some(s"$uri/1=e")
    )
    // write all partitions in partition spec
    api.writePartition(
      df=List("g").toDF("0"),
      tableName=tableName,
      partition=List("1"->"h", "2"->"i"),
      uri=Some(uri)
    )
    api.readTable(tableName=tableName, uri=Some(uri)).orderBy("0")
      .collect() should be (List(
      Row("a","b","c"),
      Row("d","e","f"),
      Row("g","h","i")))
  }

  it must "write undefined global table partition" in {
    import spark.implicits._
    val (tableName, version) = ("write_partition", "v1")
    // make sure table doesn't exist
    Hadoop.rm(s"$globalTablesDir/$tableName")
    // write
    api.writePartition(
      df=List("a").toDF,
      tableName=tableName,
      partition=List("_c1"->"b"),
      version=Some(version)
    )
    api.readTable(tableName, Some(version))
      .collect() should be (List(Row("a", "b")))
    api.readTable(tableName)
      .collect() should be (List(Row("a", "b")))
  }

  it must "write and overwrite defined global table partitions" in {
    import spark.implicits._
    val (tableName, version) = ("write_partition", "v3")
    val uri = s"$globalTablesDir/$tableName/$version"
    // create table in catalog
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE `${tableName}_$version`(`0` int)
         |PARTITIONED BY (`1` string, `2` string)
         |STORED AS PARQUET
         |LOCATION '$uri'
      """.stripMargin)
    // create view of table without version
    spark.sql(
      s"""
         |CREATE VIEW `$tableName`
         |AS SELECT * FROM `${tableName}_$version`
       """.stripMargin)
    // make sure table doesn't exist
    Hadoop.rm(uri)
    // write
    api.writePartition(
      df=List(0).toDF("0"),
      tableName=tableName,
      partition=List("1"->"a", "2"->"b"),
      version=Some(version)
    )
    api.readTable(tableName).collect() should be (List(Row(0, "a", "b")))
    // overwrite
    api.writePartition(
      df=List((1,"b")).toDF("0", "2"),
      tableName=tableName,
      partition=List("1"->"a"),
      version=Some(version),
      writeConfig={w=>w.mode("overwrite").partitionBy("2")}
    )
    api.readTable(tableName).collect() should be (List(Row(1, "a", "b")))
  }
}
