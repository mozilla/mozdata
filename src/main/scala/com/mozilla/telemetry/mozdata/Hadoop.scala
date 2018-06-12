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
    * @return Some((dirnames, filenames)) found in the directory or
    *         None if the path doesn't exist. omits symlinks.
    */
  def ls(directory: String): Option[(List[String], List[String])] = {
    val path = new Path(directory)
    val fs = FileSystem.get(path.toUri, new Configuration())
    if (!fs.exists(path)) {None} else {
      val listing = fs.listStatus(path).toList
      val dirNames = listing.collect {
        case status if status.isDirectory => status.getPath.getName
      }
      val fileNames = listing.collect {
        case status if status.isFile => status.getPath.getName
      }
      Some((dirNames, fileNames))
    }
  }

  def lsOrNil(directory: String): (List[String], List[String]) =
    ls(directory).getOrElse((Nil, Nil))

  /** Delete a directory or file using hadoop
    *
    * @param pathString path to the directory or file to delete using hadoop
    */
  def rm(pathString: String, recursive: Boolean=true): Unit = {
    val path = new Path(pathString)
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
  def read(file: String): String = {
    val path = new Path(file)
    val fs = FileSystem.get(path.toUri, new Configuration())
    val fp = fs.open(path)
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
  def write(file: String, body: String): Unit = {
    val path = new Path(file)
    val fs = FileSystem.get(path.toUri, new Configuration())
    val fp = fs.create(path)
    try {
      fp.write(body.getBytes())
    } finally {
      fp.close()
    }
  }
}
