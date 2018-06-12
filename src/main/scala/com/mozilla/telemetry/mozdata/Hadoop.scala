/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.mozdata

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Hadoop {
  /** List the directories and files within a directory using hadoop
    *
    * @param directory path to the directory to list
    * @return (dirnames, filenames) found in the directory. omits symlinks.
    */
  def ls(directory: String): (List[String], List[String]) = ls(new Path(directory))
  def ls(directory: Path): (List[String], List[String]) = {
    val fs = FileSystem.get(directory.toUri, new Configuration())
    if (fs.exists(directory)) {
      val listing = fs.listStatus(directory).toList
      val dirNames = listing.collect {
        case status if status.isDirectory => status.getPath.getName
      }
      val fileNames = listing.collect {
        case status if status.isFile => status.getPath.getName
      }
      (dirNames, fileNames)
    } else {
      (List(), List())
    }
  }

  /** Delete a directory or file using hadoop
    *
    * @param path path to the directory or file to delete using hadoop
    */
  def rm(path: String): Unit = rm(new Path(path), recursive=true)
  def rm(path: String, recursive: Boolean): Unit = rm(new Path(path), recursive)
  def rm(path: Path): Unit = rm(path, recursive=true)
  def rm(path: Path, recursive: Boolean): Unit = {
    val fs = FileSystem.get(path.toUri, new Configuration())
    if (fs.exists(path)) {
      fs.delete(path, recursive)
    }
  }

  /** Read utf-8 file contents using hadoop
    *
    * @param file path to the file to read
    * @return contents of file, decoded using utf-8
    */
  def read(file: String): String = read(new Path(file))
  def read(file: Path): String = {
    val fs = FileSystem.get(file.toUri, new Configuration())
    val fp = fs.open(file)
    try {
      IOUtils.toString(fp, "UTF-8")
    } finally {
      fp.close()
    }
  }

  /** Write file contents using hadoop
    *
    * @param file path to the file to write
    * @param body content to write
    */
  def write(file: String, body: String): Unit = write(new Path(file), body)
  def write(file: String, body: Array[Byte]): Unit = write(new Path(file), body)
  def write(file: Path, body: String): Unit = write(file, body.getBytes)
  def write(file: Path, body: Array[Byte]): Unit = {
    val fs = FileSystem.get(file.toUri, new Configuration())
    val fp = fs.create(file)
    try {
      fp.write(body)
    } finally {
      fp.close()
    }
  }
}
