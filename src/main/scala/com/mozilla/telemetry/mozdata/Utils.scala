/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.mozdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import scala.util.matching.Regex

object Utils {
  final val LocationInCreateTableStatementRegex: Regex = "(?s).*LOCATION[^']+'([^']+)'.*".r

  def flattenValues[T,U](input: Map[T,Option[U]]): Map[T,U] = input.collect {
    case (k, Some(v)) => k -> v
  }

  /** check if version is in MozData version format */
  def isVersion(version: String): Boolean = version.matches("v[0-9]+")

  /** try to get table version from uri */
  def uriVersion(uri: Option[String]): Option[String] = {
    uri.getOrElse("").split("/").filter(!_.contains('=')).last match {
      case version if isVersion(version) => Some(version)
      case _ => None
    }
  }

  /** list tables available in spark sql
    *
    * @param spark spark session used to access data
    * @param tableName optional specific table name to check for
    */
  def sparkListTables(spark: SparkSession,
                      tableName: Option[String] = None): List[String] = {
    spark
      .sql(s"SHOW TABLES ${tableName.map(t=>s"'$t'").getOrElse("")}")
      .select("tableName")
      .collect()
      .map{t => t.getString(0)}
      .toList
  }

  /** Extract non-catalog table info
    *
    * reusable code for getTableInfo
    *
    * @param tableUri uri of table without version
    * @param version optional specific version of table
    * @return TableInfo(uri, version) based on detected version
    */
  def getFsTable(tableUri: String, version: Option[String]): TableInfo = {
    val detectedVersion = version.getOrElse(
      Hadoop
        .lsOrNil(tableUri)._1
        .filter(isVersion) // versions present in hadoop
        .padTo(1, "v1") // new table with no versions fall back to "v1" for writing
        .maxBy(_.substring(1).toInt) // latest
    )
    TableInfo(Some(s"$tableUri/$detectedVersion"), Some(detectedVersion))
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
    * @return TableInfo(uri, version, sqlTableName) =>
    *         uri: detected uri, may be None if sqlTableName was found,
    *              but does not represent an external table
    *         version: detected version, may be None if version was not
    *                  specified and could not be detected
    *         sqlTableName: name that should be used to reference the
    *                       table in sql, suffixed with _${version} if
    *                       version was specified, only present for
    *                       global tables (no owner or uri specified)
    *                       that are found in the catalog (table found
    *                       by running "SHOW TABLES '$sqlTableName'")
    */
  def getTableInfo(tableName: String, version: Option[String],
                   owner: Option[String], uri: Option[String],
                   spark: SparkSession, adHocTablesDir: String,
                   globalTablesDir: String): TableInfo = (uri, owner) match {
    // uri provided
    case (Some(u), _) => TableInfo(uri, uriVersion(uri))
    // owner provided
    case (_, Some(o)) => getFsTable(s"$adHocTablesDir/$o/$tableName", version)
    // global table
    case _ =>
      val sqlTableName = version match {
        case Some(value) => s"${tableName}_$value"
        case None => tableName
      }
      if (sparkListTables(spark, Some(sqlTableName)).length < 1) {
        // table does not exist in catalog
        getFsTable(s"$globalTablesDir/$tableName", version)
      } else {
        // table exists in catalog
        try {
          // sql throws NoSuchTableException if table is temporary or a view
          val createTableStatement = spark
            .sql(s"SHOW CREATE TABLE `$sqlTableName`")
            .select("createtab_stmt")
            .take(1)
            .head
            .getString(0)

          createTableStatement match {
            // found location
            case LocationInCreateTableStatementRegex(location) => TableInfo(
              Some(location),
              uriVersion(Some(location)),
              Some(sqlTableName)
            )
            // throw NoSuchTableException because table is not external
            case _ => throw new NoSuchTableException(db="default", table=sqlTableName)
          }
        } catch {
          // no external table by that name
          case _: NoSuchTableException => TableInfo(None, version, Some(sqlTableName))
        }
      }
  }

  case class TableInfo(uri: Option[String] = None, version: Option[String] = None,
                       sqlTableName: Option[String] = None) {
    // whether or not this table is in the catalog
    val inCatalog: Boolean = sqlTableName.isDefined
  }
}
