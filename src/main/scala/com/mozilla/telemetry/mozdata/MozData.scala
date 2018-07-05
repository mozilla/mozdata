/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.mozdata

import java.util.UUID.randomUUID

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.mozdata.Utils.{flattenValues,getTableInfo,isVersion,sparkListTables}
import com.mozilla.telemetry.utils.S3Store
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.{DefaultFormats, Formats}
import org.apache.spark.sql.{DataFrame,DataFrameReader,DataFrameWriter,Row,SparkSession}
import scalaj.http.Http

import scala.collection.immutable.ListMap
import scala.io.Source

/** A consistent API for accessing Mozilla data that reports usage to Mozilla
  *
  * example:
  *
  * // create a custom dau by channel table
  * val api = MozData(spark)
  * api.writeTable(
  *   df=api.readTable("main_summary")
  *     .where("submission_date_s3='20180101'")
  *     .groupBy("submission_date_s3", "channel")
  *     .agg(countDistinct("client_id").as("dau"))
  *     .drop("submission_date_s3"),
  *   tableName="dau_by_channel",
  *   partitionValues=List(("submission_date_s3", "20180101")),
  *   owner=Some("nobody@mozilla.com")
  * )
  *
  * @param spark spark session used to access data
  * @param adHocTablesDir filesystem location of ad hoc tables
  * @param globalTablesDir filesystem location of global tables
  * @param defaultMetadataUpdateMethods default list of methods to use when
  *                                     updating table metadata, default is
  *                                     List("sql_repair", "sql_refresh")
  * @param readConfig optional function used to configure all DataFrameReaders
  * @param writeConfig optional function used to configure all DataFrameWriters
  * @param telemetryUrl optional url where logs should be posted
  */
class MozData(spark: SparkSession, adHocTablesDir: String, globalTablesDir: String,
              defaultMetadataUpdateMethods: List[String],
              readConfig: Function[DataFrameReader,DataFrameReader],
              writeConfig: Function[DataFrameWriter[Row],DataFrameWriter[Row]],
              telemetryUrl: Option[String]){
  final val listRDDsBucket: String = "net-mozaws-prod-us-west-2-pipeline-metadata"
  val apiVersion: String = Source.fromFile("VERSION").mkString
  val logger: Logger = Logger.getLogger(classOf[MozData])
  if (telemetryUrl.isDefined) {
    logger.debug(s"telemetryUrl: ${telemetryUrl.get}")
  }

  /** Report an interaction with this api */
  private def log(action: String, event: Map[String, Option[String]]): Unit = {
    implicit val formats: Formats = DefaultFormats
    val ping: String = write(ListMap((
      flattenValues(event) + ("apiVersion" -> apiVersion, "apiCall" -> action)
    ).toList.sorted:_*))
    logger.debug(s"$ping")
    if (telemetryUrl.isDefined) {
      val url = s"${telemetryUrl.get}/submit/mozdata/event/1/${randomUUID.toString}"
      Http(url).postData(ping).header("content-type", "application/json").asString
    }
  }

  /** Describe a table available to readTable
    *
    * @param tableName table to describe
    * @param version optional specific version of table, defaults to "v1" for
    *                new tables and the latest version for existing tables
    * @param owner optional email that identifies non-global namespace
    * @param uri optional non-standard location for this table
    */
  def describeTable(tableName: String, version: Option[String] = None,
                    owner: Option[String] = None, uri: Option[String] = None): Map[String,String] = {
    val tableInfo = getTableInfo(
      tableName=tableName,
      version=version,
      owner=owner,
      uri=uri,
      spark=spark,
      adHocTablesDir=adHocTablesDir,
      globalTablesDir=globalTablesDir
    )

    log(
      action="describeTable",
      Map(
        "detectedUri" -> tableInfo.uri,
        "detectedVersion" -> tableInfo.version,
        "owner" -> owner,
        "sqlTableName" -> tableInfo.sqlTableName,
        "tableName" -> Some(tableName),
        "version" -> version
      )
    )

    flattenValues(Map(
      "sqlTableName" -> tableInfo.sqlTableName,
      "detectedUri" -> tableInfo.uri,
      "detectedVersion" -> tableInfo.version
    )) ++ (if (tableInfo.sqlTableName.isDefined) {
      spark
        .sql(s"SHOW TBLPROPERTIES `${tableInfo.sqlTableName.get}`")
        .collect
        .map{r=>r.getAs[String]("key")->r.getAs[String]("value")}
        .filter(!_._1.startsWith("transient_"))
        .toMap
    } else {Map()})
  }

  /** List the rdds available to readRDD
    *
    * example:
    *
    * // list raw dataset names
    * val api = MozData(spark)
    * api.listRDDs().foreach(_("name"))
    *
    * @return list of source metadata objects, each updated with name of source
    */
  def listRDDs(): List[Map[String,String]] = {
    log("listRDDs", Map("sourcesJson" -> Some(s"s3://$listRDDsBucket/sources.json")))

    implicit val formats: Formats = DefaultFormats
    val sources = read[Map[String,Map[String,String]]](
      Source.fromInputStream(S3Store.getKey(listRDDsBucket, "sources.json")).mkString
    )

    sources.map{kv => kv._2 + ("name" -> kv._1)}.toList
  }

  /** List the tables available to readTable
    *
    * example:
    *
    * val api = MozData(spark)
    *
    * // list global tables
    * api.listTables()
    *
    * // list nobody@mozilla.com's tables
    * api.listTables(owner=Some("nobody@mozilla.com"))
    *
    * @param owner optional email that identifies non-global namespace
    * @return list of table names
    */
  def listTables(owner: Option[String] = None): List[String] = {
    log("listTables", Map(
      "owner" -> owner,
      "adHocTablesDir" -> owner.map(_=>adHocTablesDir)
    ))
    if (owner.isDefined) {
      val tablesUri = s"$adHocTablesDir/${owner.get}"
      Hadoop.lsOrNil(tablesUri)._1.filter { table =>
        Hadoop.lsOrNil(s"$tablesUri/$table")._1.exists(isVersion)
      }
    } else {
      sparkListTables(spark)
    }
  }

  /** Read a raw dataset
    *
    * example:
    *
    * // read a little bit of raw telemetry
    * val api = MozData(spark)
    * val rdd = api.readRDD(
    *   "telemetry",
    *   where={_.where("sourceVersion"){case "4" => true}},
    *   fileLimit=1
    * )
    *
    * @param name dataset source name
    * @param where clauses passed to Dataset.where
    * @param fileLimit passed to Dataset.records
    * @param minPartitions passed to Dataset.records
    * @return Messages read
    */
  def readRDD(name: String, where: Function[Dataset,Dataset] = identity,
              fileLimit: Option[Int] = None,
              minPartitions: Option[Int] = None): RDD[Message] = {
    log("readRDD", Map("name" -> Some(name)))
    where(Dataset(name)).records(fileLimit, minPartitions)(spark.sparkContext)
  }

  /** Read a table
    *
    * example:
    *
    * val api = MozData(spark)
    *
    * // read a global table
    * val clientsDaily = api.readTable("clients_daily")
    *
    * // read v1 of nobody@mozilla.com's special_dau table
    * val specialDauV1 = api.readTable(
    *   tableName="special_dau",
    *   owner=Some("nobody@mozilla.com"),
    *   version=Some("v1"),
    *   extraReadConfig={_.option("mergeSchema", "true")}
    * )
    *
    * // read a json special_dau table defined by an s3 path
    * val specialDauV2 = api.readTable(
    *   tableName="special_dau",
    *   uri=Some("s3://special-bucket/special_dau/v2"),
    *   extraReadConfig={_.format("json")}
    * )
    *
    * @param tableName table to read
    * @param version optional specific version of table, defaults to "v1" for
    *                new tables and the latest version for existing tables
    * @param owner optional email that identifies non-global namespace
    * @param uri optional non-standard location for this table
    * @param extraReadConfig optional function to configure the DataFrameReader
    * @return DataFrame of the requested table
    */
  def readTable(tableName: String, version: Option[String] = None,
                owner: Option[String] = None, uri: Option[String] = None,
                extraReadConfig: Function[DataFrameReader,DataFrameReader] = identity
               ): DataFrame = {
    val tableInfo = getTableInfo(
      tableName=tableName,
      version=version,
      owner=owner,
      uri=uri,
      spark=spark,
      adHocTablesDir=adHocTablesDir,
      globalTablesDir=globalTablesDir
    )

    log(
      action="readTable",
      Map(
        "detectedUri" -> tableInfo.uri,
        "detectedVersion" -> tableInfo.version,
        "owner" -> owner,
        "sqlTableName" -> tableInfo.sqlTableName,
        "tableName" -> Some(tableName),
        "uri" -> uri,
        "version" -> version
      )
    )

    val reader = extraReadConfig(readConfig(spark.read))

    if (tableInfo.inCatalog) {
      reader.table(tableInfo.sqlTableName.get)
    } else {
      reader.load(tableInfo.uri.get)
    }
  }

  /** Execute a SparkSQL query */
  def sql(query: String): DataFrame = {
    log("sql", Map("query" -> Some(query)))
    spark.sql(query)
  }

  /** Write table to long term storage
    *
    * example:
    *
    * val api = MozData(spark)
    * val myDF = (0 to 5)
    *   .map(v=>("20180101", v, "beta"))
    *   .toDF("submission_date_s3", "test_value", "channel")
    *
    * // append new partitions to a global table
    * api.writeTable(
    *   df=myDF,
    *   tableName="clients_daily",
    *   version=Some("v4"),
    *   extraWriteConfig={_.mode("append").partitionBy("submission_date_s3")},
    *   // not a partitioned table, so exclude "sql_repair" from update methods
    *   metadataUpdateMethods=List("sql_refresh")
    * )
    *
    * // write a single date to the latest version of nobody@mozilla.com's special_dau table
    * api.writeTable(
    *   df=myDF.where("submission_date_s3='20180101'").drop("submission_date_s3"),
    *   tableName="special_dau",
    *   partitionValues=List(("submission_date_s3", "20180101")),
    *   owner=Some("nobody@mozilla.com"),
    *   extraWriteConfig={_.mode("overwrite").partitionBy("channel")}
    * )
    *
    * // write a json table to a specific s3 path
    * api.writeTable(
    *   df=myDF.where("channel='beta'").drop("channel"),
    *   tableName="special_dau",
    *   uri=Some("s3://special-bucket/special_dau/v2"),
    *   extraReadConfig={_.format("json")},
    * )
    *
    * // write a non-partitioned global table
    * api.writeTable(
    *   df=myDF,
    *   tableName="special_list",
    *   version=Some("v1"),
    *   extraWriteConfig={_.mode("overwrite")},
    *   // not a partitioned table, so exclude "sql_repair" from update methods
    *   metadataUpdateMethods=List("sql_refresh")
    * )
    *
    * @param df DataFrame to write
    * @param tableName table to write
    * @param partitionValues optional ordered list of key-value static partition
    *                        identifiers, which must be absent from df
    * @param version specific version of table, required for global tables,
    *                defaults to latest or "v1" if latest can't be determined
    * @param owner optional email that identifies non-global namespace
    * @param uri optional non-standard location for this table
    * @param metadataUpdateMethods optional methods to use to update metadata
    *                               after writing partitioned global tables,
    *                               default is List("sql_repair", "sql_refresh")
    *                               WARNING default "sql_repair" method
    *                               uses "MSCK REPAIR TABLE" which will throw
    *                               an exception if the table is not partitioned
    * @param extraWriteConfig optional function to configure the DataFrameWriter
    */
  def writeTable(df: DataFrame, tableName: String,
                 partitionValues: List[(String,String)] = Nil,
                 version: Option[String] = None, owner: Option[String] = None,
                 uri: Option[String] = None,
                 extraWriteConfig: Function[DataFrameWriter[Row],DataFrameWriter[Row]] = identity,
                 metadataUpdateMethods: List[String] = defaultMetadataUpdateMethods): Unit = {
    require(
      version.isDefined || owner.isDefined || uri.isDefined,
      "version required to write global table"
    )

    val tableInfo = getTableInfo(
      tableName=tableName,
      version=version,
      owner=owner,
      uri=uri,
      spark=spark,
      adHocTablesDir=adHocTablesDir,
      globalTablesDir=globalTablesDir
    )

    require(
      tableInfo.uri.isDefined,
      s"table is not external: ${tableInfo.sqlTableName.getOrElse(tableName)}"
    )


    // maybe find partition string
    val partitionValuesString = partitionValues
      .map{p=>s"${p._1}=${p._2}"}
      .reduceLeftOption((a: String, b) => s"$a/$b")

    // build uri
    val detectedUri: String = List(
      tableInfo.uri,
      partitionValuesString
    ).flatten.mkString("/")

    if (!tableInfo.inCatalog && owner.isEmpty && uri.isEmpty) {
      logger.warn(s"writing non-catalog global table: $detectedUri")
    }

    log(
      "writeTable",
      Map(
        "detectedUri" -> Some(detectedUri),
        "detectedVersion" -> tableInfo.version,
        "owner" -> owner,
        "partition" -> partitionValuesString,
        "sqlTableName" -> tableInfo.sqlTableName,
        "tableName" -> Some(tableName),
        "uri" -> uri,
        "version" -> version
      )
    )

    extraWriteConfig(writeConfig(df.write)).save(detectedUri)

    if (tableInfo.inCatalog) {
      // update metadata on catalog tables
      metadataUpdateMethods.foreach{
        case "sql_repair" => spark.sql(s"MSCK REPAIR TABLE `${tableInfo.sqlTableName.get}`")
        case "sql_refresh" => spark.sql(s"REFRESH TABLE `${tableInfo.sqlTableName.get}`")
        case value => throw new IllegalArgumentException(s"Unsupported metadata location: $value")
      }
    }
  }
}

object MozData {
  def apply(spark: SparkSession,
            adHocTablesDir: String = sys.env.getOrElse(
              "AD_HOC_TABLES_DIR",
              "s3://net-mozaws-prod-us-west-2-pipeline-analysis"
            ),
            globalTablesDir: String = sys.env.getOrElse(
              "GLOBAL_TABLES_DIR",
              "s3://telemetry-parquet"
            ),
            defaultMetadataUpdateMethods: List[String] = sys.env.getOrElse(
              "DEFAULT_METADATA_UPDATE_METHODS",
              "sql_repair,sql_refresh"
            ).split(",").toList,
            readConfig: Function[DataFrameReader,DataFrameReader] = identity,
            writeConfig: Function[DataFrameWriter[Row],DataFrameWriter[Row]] = identity,
            telemetryUrl: Option[String] = sys.env.get("TELEMETRY_URL")
           ): MozData = new MozData(
    spark=spark,
    adHocTablesDir=adHocTablesDir,
    globalTablesDir=globalTablesDir,
    defaultMetadataUpdateMethods=defaultMetadataUpdateMethods,
    readConfig=readConfig,
    writeConfig=writeConfig,
    telemetryUrl=telemetryUrl
  )
}
