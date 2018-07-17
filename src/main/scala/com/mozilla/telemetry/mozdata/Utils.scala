/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.mozdata

import org.apache.spark.sql._//{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import scala.util.matching.Regex

object Utils {
  final val LocationInCreateTableStatementRegex: Regex = "(?s).*LOCATION[^']+'([^']+)'.*".r

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

  /** check if version is in MozData version format */
  def isVersion(version: String): Boolean = version.matches("v[0-9]+")

  private[mozdata] class Table(spark: SparkSession, adHocTablesDir: String, globalTablesDir: String,
                      tableName: String, version: Option[String], owner: Option[String],
                      uri: Option[String], partitionValues: List[(String,String)] = Nil) {
    lazy val partitionValuesStrings: List[String] = partitionValues
      .map{p=>s"${p._1}=${p._2}"}

    lazy val sqlTableName: String = version match {
      case Some(value) => s"${tableName}_$value"
      case None => tableName
    }

    lazy val uriVersion: Option[String] = uri
      .map(_.split("/").filter(!_.contains('=')))
      .filter(_.nonEmpty)
      .map(_.last)
      .filter(isVersion)
      .orElse(version)

    lazy val tableUri: String = owner match {
      case Some(o) => s"$adHocTablesDir/$owner/$tableName"
      case _ => s"$globalTablesDir/$tableName"
    }

    lazy val tableUriVersion: String = version.getOrElse(Hadoop
      .lsOrNil(tableUri)._1
      .filter(isVersion)
      .padTo(1, "v1")
      .maxBy(_.substring(1).toInt))

    lazy val tableUriWithVersion = s"$tableUri/$tableUriVersion"

    val readVersion: Option[String] = (uri, owner) match {
      case (Some(u), _) => uriVersion
      case (_, Some(_)) => Some(tableUriVersion)
      case _ => version
    }

    val readUriWithoutPartition: String = (uri, owner) match {
      case (Some(u), _) => u
      case (_, Some(_)) => tableUriWithVersion
      case _ => sqlTableName // breaks with partitionValues
    }

    val readUri: String = (readUriWithoutPartition :: partitionValuesStrings).mkString("/")

    val readWithLoad: Boolean = uri.isDefined || owner.isDefined

    lazy val existsInCatalog: Boolean = sparkListTables(spark, Some(sqlTableName)).nonEmpty

    lazy val writeUriWithoutPartition: String = (uri, owner) match {
      case (Some(u), _) => u
      case (_, Some(_)) => tableUriWithVersion
      case _ if !existsInCatalog => tableUriWithVersion
      case _ => try {
        // sql throws NoSuchTableException if table is temporary or a view
        val createTableStatement = spark
          .sql(s"SHOW CREATE TABLE `$sqlTableName`")
          .select("createtab_stmt")
          .take(1)
          .head
          .getString(0)

        createTableStatement match {
          // found location
          case LocationInCreateTableStatementRegex(location) => location
          // throw NoSuchTableException because table is not external
          case _ => throw new NoSuchTableException(db="default", table=sqlTableName)
        }
      } catch {
        // no external table by that name
        case _: NoSuchTableException => throw new IllegalArgumentException(s"table is not external: $sqlTableName")
      }
    }

    val writeUri: String = (writeUriWithoutPartition :: partitionValuesStrings).mkString("/")

    lazy val writeVersion: Option[String] = owner match {
      case Some(_) => Some(tableUriVersion)
      case _  => version
    }

    lazy val updateCatalog: Boolean = owner.isDefined
  }
}
