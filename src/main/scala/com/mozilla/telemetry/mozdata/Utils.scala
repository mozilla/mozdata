/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.mozdata

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization.write

import scala.collection.immutable.ListMap
import scala.io.Source
import scala.util.matching.Regex

object Utils {
  val locationRegex: Regex = "(?s).*LOCATION[^']+'([^']+)'.*".r

  /** Update metadata for table */
  def updateMetadata(sqlTableName: String, method: String,
                     spark: SparkSession): Unit = method match {
    case "sql_repair" => spark.sql(s"MSCK REPAIR TABLE `$sqlTableName`")
    case "sql_refresh" => spark.sql(s"REFRESH TABLE `$sqlTableName`")
    // TODO support "lambda" method
    // TODO support "glue" method
    case value => throw new IllegalArgumentException(s"Unsupported metadata location: $value"
    )
  }

  /** check if version is in MozData version format */
  def isVersion(version: String): Boolean = version.matches("v[0-9]+")

  /** try to get table version from uri */
  def uriVersion(uri: Option[String]): Option[String] = {
    uri.getOrElse("").split("/").last match {
      case version if isVersion(version) => Some(version)
      case _ => None
    }
  }

  /** Extract non-catalog table info
    *
    * reusable code for getTableInfo
    *
    * @param tableUri uri of table without version
    * @param version optional specific version of table
    * @return same as getTableInfo
    */
  def getFsTable(tableUri: String, version: Option[String]):
  (Option[String], Option[String], Option[String]) = {
    val detectedVersion = version.getOrElse({
      val versions = Hadoop.ls(tableUri)._1.filter(isVersion)
      if (versions.isEmpty) {"v1"} // new table, fall back to version "v1"
      else {versions.maxBy(_.substring(1).toInt)} // use latest version
    })
    (Some(s"$tableUri/$detectedVersion"), Some(detectedVersion), None)
  }

  /** Locate information needed for accessing the given table
    *
    * @param tableName table to look up
    * @param version optional specific version of table
    * @param owner optional email that identifies non-global namespace
    * @param uri optional non-standard location for this table
    * @param spark spark session used to access data
    * @param adHocTablesDir filesystem location of ad hoc tables
    * @param globalTablesDir filesystem location of global tables
    * @return (detectedUri, detectedVersion, sqlTableName) of table, at
    *         least one of detectedUri or sqlTableName will be defined
    */
  def getTableInfo(tableName: String, version: Option[String],
                   owner: Option[String], uri: Option[String],
                   spark: SparkSession, adHocTablesDir: String,
                   globalTablesDir: String):
  (Option[String], Option[String], Option[String]) = (uri, owner) match {
    // uri provided
    case (Some(u), _) => (uri, uriVersion(uri), None)
    // owner provided
    case (_, Some(o)) => getFsTable(s"$adHocTablesDir/$o/$tableName", version)
    // global table
    case _ =>
      val sqlTableName = version match {
        case Some(value) => s"${tableName}_$value"
        case None => tableName
      }
      if (spark.sql(s"SHOW TABLES '$sqlTableName'").collect.length < 1) {
        // table does not exist in catalog
        getFsTable(s"$globalTablesDir/$tableName", version)
      } else {
        // table exists in catalog
        try {
          // sql throws NoSuchTableException if table is temporary or a view
          val createStmt = spark
            .sql(s"SHOW CREATE TABLE `$sqlTableName`")
            .collect()
            .head
            .getAs[String]("createtab_stmt")

          createStmt match {
            // found location
            case locationRegex(location) => (
              Some(location),
              uriVersion(Some(location)),
              Some(sqlTableName)
            )
            // throw NoSuchTableException because table is not external
            case _ => throw new NoSuchTableException("default", sqlTableName)
          }
        } catch {
          // no external table by that name
          case _: NoSuchTableException => (None, version, Some(sqlTableName))
        }
      }
  }
}
