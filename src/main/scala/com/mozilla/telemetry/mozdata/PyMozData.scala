/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.mozdata

import org.apache.spark.sql._

import scala.collection.JavaConverters._

class PyMozData(spark: SparkSession, adHocTablesDir: String,
                globalTablesDir: String,
                defaultMetadataUpdateMethods: java.util.ArrayList[String]) {
  // type converters
  private def listFromScala[T](input: List[T]): java.util.ArrayList[T] =
    new java.util.ArrayList[T](input.asJava)

  private def listOfMapsFromScala(input: List[Map[String,String]]):
  java.util.ArrayList[java.util.HashMap[String,String]] = listFromScala(
    input.map{m => new java.util.HashMap[String, String](m.asJava)}
  )

  private def listToScala[T](input: java.util.ArrayList[T]): List[T] =
    input.asScala.toList

  private def mapToScala[T](input: java.util.HashMap[String,T]): Map[String,T] =
    Map(input.asScala.toList: _*)

  private def mapOfListsToScala(input: java.util.HashMap[String,java.util.ArrayList[String]]):
  Map[String,List[String]] = mapToScala(input).map{kv => kv._1->kv._2.asScala.toList}

  private def listOfMapsToScala(input: java.util.ArrayList[java.util.HashMap[String,String]]):
  List[Map[String,String]] = listToScala(input).map{m => mapToScala(m)}

  private def listOfTuplesToScala(input: java.util.ArrayList[java.util.ArrayList[String]]):
  List[(String,String)] = listToScala(input).map{ pair =>
    val scalaPair = pair.asScala.toList
    (scalaPair.headOption.orNull, scalaPair.lift(1).orNull)
  }

  private def readConfigAsFunction(javaOptions: java.util.HashMap[String,String]
                                  ): Function[DataFrameReader,DataFrameReader] = {
    val options: Map[String,String] = mapToScala(javaOptions)
    (r: DataFrameReader) => {
      options.foldLeft(r)((r, kv) => kv match {
        case ("format", v) => r.format(v)
        // TODO expose r.schema
        //case ("schema", List(v)) => r.schema(v)
        case (k, v) => r.option(k, v)
        case o => throw new IllegalArgumentException(s"unsupported write option: $o")
      })
    }
  }

  private def writeConfigAsFunction(
    javaOptions: java.util.HashMap[String,java.util.ArrayList[String]]
  ): Function[DataFrameWriter[Row],DataFrameWriter[Row]] = {
    val options: Map[String,List[String]] = mapOfListsToScala(javaOptions)
    (w: DataFrameWriter[Row]) => {
      options.foldLeft(w)((w, kv) => kv match {
        case ("bucketBy", v) =>
          require(
            v.head.matches("[0-9]+"),
            "first argument to bucketBy must be integer"
          )
          val cols = v.tail
          w.bucketBy(v.head.toInt, cols.head, cols.tail:_*)
        case ("format", List(v)) => w.format(v)
        case ("mode", List(v)) => w.mode(v)
        case ("partitionBy", v) => w.partitionBy(v:_*)
        case ("sortBy", v) => w.sortBy(v.head, v.tail:_*)
        case (k, List(v)) => w.option(k, v)
        case o => throw new IllegalArgumentException(s"unknown write option: $o")
      })
    }
  }

  // MozData instance
  val api = MozData(
    spark=spark,
    adHocTablesDir=adHocTablesDir match {
      case null => MozData.defaultAdHocTablesDir
      case _ => adHocTablesDir
    },
    globalTablesDir=globalTablesDir match {
      case null => MozData.defaultGlobalTablesDir
      case _ => globalTablesDir
    },
    defaultMetadataUpdateMethods=defaultMetadataUpdateMethods match {
      case null => MozData.defaultMetadataUpdateMethods
      case _ => listToScala(defaultMetadataUpdateMethods)
    }
  )

  // python interface
  def listRDDs(): java.util.ArrayList[java.util.HashMap[String,String]] =
    listOfMapsFromScala(api.listRDDs())

  def logListRDDs(): Unit =
    api.log("list_rdds")

  def listTables(owner: String): Array[String] =
    api.listTables(owner=Option(owner)).toArray

  def logReadRDD(name: String): Unit =
    api.log("read_rdd", Map("name"->Some(name)))

  def readTable(tableName: String, version: String, owner: String, uri: String,
                readConfig: java.util.HashMap[String,String]
               ): DataFrame = api.readTable(
    tableName=tableName,
    version=Option(version),
    owner=Option(owner),
    uri=Option(uri),
    readConfig=readConfig match {
      case null => r=>r
      case _ => readConfigAsFunction(readConfig)
    }
  )

  def sql(query: String): DataFrame = api.sql(query=query)

  def writePartition(df: DataFrame, tableName: String,
                     partition: java.util.ArrayList[java.util.ArrayList[String]],
                     version: String, owner: String, uri: String,
                     metadataUpdateMethods: java.util.ArrayList[String],
                     writeConfig: java.util.HashMap[String,java.util.ArrayList[String]]
                    ): Unit = api.writePartition(
    df=df,
    tableName=tableName,
    partition=partition match {
      case null => List()
      case _ => listOfTuplesToScala(partition)
    },
    version=Option(version),
    owner=Option(owner),
    uri=Option(uri),
    metadataUpdateMethods=metadataUpdateMethods match {
      case null => None
      case _ => Some(listToScala(metadataUpdateMethods))
    },
    writeConfig=writeConfig match {
      case null => w => w
      case _ => writeConfigAsFunction(writeConfig)
    }
  )

  def writeTable(df: DataFrame, tableName: String, version: String,
                 owner: String, uri: String,
                 metadataUpdateMethods: java.util.ArrayList[String],
                 writeConfig: java.util.HashMap[String,java.util.ArrayList[String]]
                ): Unit = api.writeTable(
    df=df,
    tableName=tableName,
    version=Option(version),
    owner=Option(owner),
    uri=Option(uri),
    metadataUpdateMethods=metadataUpdateMethods match {
      case null => None
      case _ => Some(listToScala(metadataUpdateMethods))
    },
    writeConfig=writeConfig match {
      case null => w => w
      case _ => writeConfigAsFunction(writeConfig)
    }
  )
}
