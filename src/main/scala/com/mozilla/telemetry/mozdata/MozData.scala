/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.mozdata

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.mozdata.Utils._
import com.mozilla.telemetry.utils.S3Store
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.{DefaultFormats, Formats}
import org.apache.spark.sql._

import scala.collection.immutable.ListMap
import scala.io.Source

/** A consistent API for accessing Mozilla data that reports usage to Mozilla
  *
  * @param spark spark session used to access data
  * @param adHocTablesDir filesystem location of ad hoc tables
  * @param globalTablesDir filesystem location of global tables
  * @param defaultMetadataUpdateMethods default list of methods to use when
  *                                     updating table metadata
  */
class MozData(spark: SparkSession, adHocTablesDir: String,
              globalTablesDir: String,
              defaultMetadataUpdateMethods: List[String]){
  val apiVersion: String = Source.fromFile("VERSION").mkString
  val logger: Logger = Logger.getLogger(classOf[MozData])

  /** Report an interaction with this api */
  private[mozdata] def log(action: String, metadata: Map[String, Option[String]] = Map()): Unit = {
    implicit val formats: Formats = DefaultFormats
    val ping: String = write(ListMap((metadata.collect {
      case (k, Some(v)) => k -> v
    } + ("apiVersion"->apiVersion)).toList.sorted:_*))
    logger.debug(s"$action: $ping")
  }

  /** List the rdds available to readRDD
    *
    * @return list of source metadata objects, each updated with name of source
    */
  def listRDDs(): List[Map[String,String]] = {
    val bucket = "net-mozaws-prod-us-west-2-pipeline-metadata"
    log("listRDDs", Map("sourcesJson"->Some(s"$bucket/sources.json")))

    implicit val formats: Formats = DefaultFormats
    val sources = read[Map[String,Map[String,String]]](
      Source.fromInputStream(S3Store.getKey(bucket, "sources.json")).mkString
    )

    sources.map{kv => kv._2 + ("name" -> kv._1)}.toList
  }

  /** List the tables available to readTable
    *
    * @param owner optional email that identifies non-global namespace
    * @return list of table names
    */
  def listTables(owner: Option[String] = None): List[String] = {
    owner match {
      case None =>
        log("listTables")
        spark
          .sql("SHOW TABLES")
          .collect()
          .map{t => t.getAs[String]("tableName")}
          .toList
      case Some(value) =>
        log("listTables", Map("owner"->owner,"adHocTablesDir"->Some(adHocTablesDir)))
        val tablesUri = s"$adHocTablesDir/$value"
        Hadoop.ls(tablesUri)._1.filter { table =>
          Hadoop.ls(s"$tablesUri/$table")._1.exists(isVersion)
        }
    }
  }

  /** Read a raw dataset
    *
    * @param name dataset source name
    * @param where clauses passed to Dataset.where
    * @param fileLimit passed to Dataset.records
    * @param minPartitions passed to Dataset.records
    * @return Messages read
    */
  def readRDD(name: String, where: Function[Dataset,Dataset] = {w=>w},
              fileLimit: Option[Int] = None,
              minPartitions: Option[Int] = None): RDD[Message] = {
    log("readRDD", Map("name"->Some(name)))
    where(Dataset(name)).records(fileLimit, minPartitions)(spark.sparkContext)
  }

  /** Read a table
    *
    * @param tableName table to read
    * @param version optional specific version of table, defaults to "v1" for
    *                new tables and the latest version for existing tables
    * @param owner optional email that identifies non-global namespace
    * @param uri optional non-standard location for this table
    * @param readConfig optional function to configure the DataFrameReader
    * @return DataFrame of the requested table
    */
  def readTable(tableName: String, version: Option[String] = None,
                owner: Option[String] = None, uri: Option[String] = None,
                readConfig: Function[DataFrameReader,DataFrameReader] = {r=>r}
               ): DataFrame = {
    val (detectedUri, detectedVersion, sqlTableName) = getTableInfo(
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
        "detectedUri" -> detectedUri,
        "detectedVersion" -> detectedVersion,
        "owner" -> owner,
        "sqlTableName" -> sqlTableName,
        "tableName" -> Some(tableName),
        "uri" -> uri,
        "version" -> version
      )
    )

    val reader = readConfig(spark.read)
    (sqlTableName, detectedUri) match {
      case (Some(table), _) => reader.table(table)
      case (None, Some(path)) => reader.load(path)
      case _ => throw new Exception(s"unable to locate table: $tableName")
    }
  }

  /** Execute a SparkSQL query */
  def sql(query: String): DataFrame = {
    log("query", Map("query"->Some(query)))
    spark.sql(query)
  }

  /** Write partition to long term storage
    *
    * @param df DataFrame to write
    * @param tableName table to write
    * @param partition ordered list of key-value static partition identifiers,
    *                  which must be absent from df. optional if specifying uri
    * @param version optional specific version of table, defaults to "v1" for
    *                new tables and the latest version for existing tables
    * @param owner optional email that identifies non-global namespace
    * @param uri optional non-standard location for this table
    * @param metadataUpdateMethods optional methods to use to update metadata
    *                               after writing partitioned global tables
    * @param writeConfig optional function to configure the DataFrameWriter
    */
  def writePartition(df: DataFrame, tableName: String,
                     partition: List[(String,String)] = List(),
                     version: Option[String] = None,
                     owner: Option[String] = None, uri: Option[String] = None,
                     writeConfig: Function[DataFrameWriter[Row],DataFrameWriter[Row]] = {w=>w},
                     metadataUpdateMethods: Option[List[String]] = None): Unit = {
    require(
      partition.nonEmpty || uri.isDefined,
      "at least one partition must be specified"
    )

    require(
      version.isDefined || owner.isDefined || uri.isDefined,
      "version required to write global table partition"
    )

    val (detectedTableUri, detectedVersion, sqlTableName) = getTableInfo(
      tableName=tableName,
      version=version,
      owner=owner,
      uri=uri,
      spark=spark,
      adHocTablesDir=adHocTablesDir,
      globalTablesDir=globalTablesDir
    )

    require(
      detectedTableUri.isDefined,
      s"table is not external: ${sqlTableName.getOrElse(tableName)}"
    )

    // maybe find partition string
    val partitionString = if (partition.nonEmpty) {
      Some(partition.map{p=>s"${p._1}=${p._2}"}.mkString("/"))
    } else {
      None
    }

    // build uri
    val detectedUri: String = List(
      Some(uri.getOrElse(detectedTableUri.get)),
      partitionString
    ).flatten.mkString("/")

    if (sqlTableName.isEmpty && owner.isEmpty && uri.isEmpty) {
      logger.warn(s"writing non-catalog global table partition: $detectedUri")
    }

    log(
      "writePartition",
      Map(
        "detectedUri"->Some(detectedUri),
        "detectedVersion"->detectedVersion,
        "owner"->owner,
        "partition"->partitionString,
        "sqlTableName"->sqlTableName,
        "tableName"->Some(tableName),
        "uri"->uri,
        "version"->version
      )
    )

    writeConfig(df.write).save(detectedUri)

    // update metadata on global tables
    if (sqlTableName.isDefined) {
      metadataUpdateMethods
        .getOrElse(defaultMetadataUpdateMethods)
        .foreach(updateMetadata(sqlTableName.get, _, spark))
    }
  }

  /** Write table to long term storage
    *
    * @param df DataFrame to write
    * @param tableName table to write
    * @param version specific version of table, required for global tables,
    *                defaults to latest or "v1" if latest can't be determined
    * @param owner optional email that identifies non-global namespace
    * @param uri optional non-standard location for this table
    * @param metadataUpdateMethods optional methods to use to update metadata
    *                               after writing partitioned global tables,
    *                               WARNING default "sql" method *requires*
    *                               table to be partitioned
    * @param writeConfig optional function to configure the DataFrameWriter
    */
  def writeTable(df: DataFrame, tableName: String,
                 version: Option[String] = None, owner: Option[String] = None,
                 uri: Option[String] = None,
                 writeConfig: Function[DataFrameWriter[Row],DataFrameWriter[Row]] = {w=>w},
                 metadataUpdateMethods: Option[List[String]] = None): Unit = {
    require(
      version.isDefined || owner.isDefined || uri.isDefined,
      "version required to write global table"
    )

    val (detectedUri, detectedVersion, sqlTableName) = getTableInfo(
      tableName=tableName,
      version=version,
      owner=owner,
      uri=uri,
      spark=spark,
      adHocTablesDir=adHocTablesDir,
      globalTablesDir=globalTablesDir
    )

    require(
      detectedUri.isDefined,
      s"table is not external: ${sqlTableName.getOrElse(tableName)}"
    )

    if (sqlTableName.isEmpty && owner.isEmpty && uri.isEmpty) {
      logger.warn(s"writing non-catalog global table: ${detectedUri.get}")
    }

    log(
      "writeTable",
      Map(
        "detectedUri"->detectedUri,
        "detectedVersion"->detectedVersion,
        "owner"->owner,
        "sqlTableName"->sqlTableName,
        "tableName"->Some(tableName),
        "uri"->uri,
        "version"->version
      )
    )

    writeConfig(df.write).save(detectedUri.get)

    // update metadata on global tables
    if (sqlTableName.isDefined) {
      metadataUpdateMethods
        .getOrElse(defaultMetadataUpdateMethods)
        .foreach(updateMetadata(sqlTableName.get, _, spark))
    }
  }
}

object MozData {
  val defaultAdHocTablesDir: String = sys.env.getOrElse(
    "AD_HOC_TABLES_DIR",
    "s3://net-mozaws-prod-us-west-2-pipeline-analysis"
  )
  val defaultGlobalTablesDir: String = sys.env.getOrElse(
    "GLOBAL_TABLES_DIR",
    "s3://telemetry-parquet"
  )
  val defaultMetadataUpdateMethods: List[String] = sys.env.getOrElse(
    "DEFAULT_METADATA_UPDATE_METHODS",
    "sql_repair,sql_refresh"
  ).split(",").toList

  def apply(spark: SparkSession,
            adHocTablesDir: String = defaultAdHocTablesDir,
            globalTablesDir: String = defaultGlobalTablesDir,
            defaultMetadataUpdateMethods: List[String] = defaultMetadataUpdateMethods
           ): MozData = new MozData(
    spark=spark,
    adHocTablesDir=adHocTablesDir,
    globalTablesDir=globalTablesDir,
    defaultMetadataUpdateMethods=defaultMetadataUpdateMethods
  )
}
