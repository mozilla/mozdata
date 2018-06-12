/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
name := "mozdata"

version := scala.io.Source.fromFile("VERSION").mkString

scalaVersion := "2.11.8"

organization := "com.mozilla.telemetry"

homepage := Some(url("http://github.com/mozilla/mozdata"))

resolvers += "S3 local maven snapshots" at "https://s3-us-west-2.amazonaws.com/net-mozaws-data-us-west-2-ops-mavenrepo/snapshots"

val sparkVersion = "2.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalaj" %% "scalaj-http" % "2.4.0",
  "com.mozilla.telemetry" %% "moztelemetry" % "1.1-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.9.0" % "test"
)

test in assembly := {}
