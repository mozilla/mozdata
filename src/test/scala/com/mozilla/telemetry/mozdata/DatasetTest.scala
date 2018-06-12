/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.mozdata

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import com.amazonaws.services.s3.model.ObjectMetadata
import com.google.protobuf.ByteString.copyFromUtf8
import com.mozilla.telemetry.heka.{Header, RichMessage}
import com.mozilla.telemetry.utils.S3Store

object DatasetTest {
  val message = RichMessage(
    "1234",
    Map(
      "bytes" -> copyFromUtf8("foo"),
      "string" -> "foo",
      "bool" -> true,
      "double" -> 4.2,
      "integer" -> 42L,
      "string-with-int-value" -> "42",
      "submission" ->
        """
          | {
          |   "partiallyExtracted" : {
          |     "alpha" : "1",
          |     "beta" : "2"
          |   },
          |   "gamma": "3"
          | }
        """.stripMargin,
      "extracted.subfield" -> """{"delta": "4"}""",
      "extracted.nested.subfield"-> """{"epsilon": "5"}""",
      "partiallyExtracted.nested" -> """{"zeta": "6"}"""
    ),
    None
  )

  val header = Header(message.toByteArray.length)

  private val framedMessage = {
    val baos = new ByteArrayOutputStream
    val bHeader = header.toByteArray
    val bMessage = message.toByteArray

    // see https://hekad.readthedocs.org/en/latest/message/index.html
    baos.write(0x1E)
    baos.write(bHeader.length)
    baos.write(bHeader, 0, bHeader.length)
    baos.write(0x1F)
    baos.write(bMessage, 0, bMessage.length)
    baos.toByteArray
  }

  def hekaFile(numRecords: Integer = 42, framedMessage: Array[Byte] = framedMessage): Array[Byte] = {
    val ba = new Array[Byte](numRecords*framedMessage.length)
    for (i <- 0 until numRecords) {
      System.arraycopy(framedMessage, 0, ba, i*framedMessage.length, framedMessage.length)
    }
    ba
  }

  private val client = S3Store.s3
  private val rddBucket = "net-mozaws-prod-us-west-2-pipeline-metadata"
  def beforeAll(): Unit = {
    client.setEndpoint("http://127.0.0.1:8001")
    client.createBucket(rddBucket)
    client.putObject(
      rddBucket,
      "sources.json",
      s"""
         |{
         |  "test": {
         |    "prefix": "test",
         |    "metadata_prefix": "test",
         |    "bucket": "$rddBucket"
         |  }
         |}
      """.stripMargin
    )
    client.putObject(
      rddBucket,
      "test/schema.json",
      s"""
         |{
         |  "dimensions": [
         |    { "field_name": "key" }
         |  ]
         |}
      """.stripMargin
    )

    def hekaStream: InputStream = new ByteArrayInputStream(hekaFile())
    client.putObject(rddBucket, "test/val1/x", hekaStream, new ObjectMetadata())
    client.putObject(rddBucket, "test/val2/x", hekaStream, new ObjectMetadata())
  }

  def afterAll(): Unit = {
    val bucket = client.bucket(rddBucket).get
    client.keys(bucket).foreach(x => client.deleteObject(rddBucket, x))
    client.deleteBucket(bucket)
  }
}
