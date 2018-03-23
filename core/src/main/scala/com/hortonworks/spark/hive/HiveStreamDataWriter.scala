/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.hive

import java.util.{Map => JMap, List => JList}
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.json4s.{DefaultFormats, Extraction}
import org.json4s.jackson.JsonMethods._

import com.hortonworks.spark.hive.common.{CachedHiveWriters, HiveOptions, HiveWriter}
import com.hortonworks.spark.hive.utils.Logging

case object HiveStreamWriterCommitMessage extends WriterCommitMessage

class HiveStreamDataWriter(
   partitionId: Int,
   attemptNumber: Int,
   columnName: Seq[String],
   partitionCols: Seq[String],
   dataSourceOptionsMap: JMap[String, String],
   initClassLoader: ClassLoader,
   isolatedClassLoader: ClassLoader) extends DataWriter[Row] with Logging {

  private implicit def formats = DefaultFormats

  private val hiveOptions =
    HiveOptions.fromDataSourceOptions(new DataSourceOptions(dataSourceOptionsMap))

  private val inUseWriters = new mutable.HashMap[Object, HiveWriter]()

  private val executorService = Executors.newSingleThreadScheduledExecutor()
  executorService.scheduleAtFixedRate(new Runnable {
    Thread.currentThread().setContextClassLoader(isolatedClassLoader)

    override def run(): Unit = {
      inUseWriters.foreach(_._2.heartbeat())
    }
  }, 10L, 10L, TimeUnit.SECONDS)

  override def write(row: Row): Unit = {
    val partitionValues = partitionCols.map { col => row.getAs[String](col) }
    val hiveEndPoint =
      Class.forName("org.apache.hive.hcatalog.streaming.HiveEndPoint", true, isolatedClassLoader)
      .getConstructor(classOf[String], classOf[String], classOf[String], classOf[JList[String]])
      .newInstance(
        hiveOptions.metastoreUri, hiveOptions.dbName, hiveOptions.tableName, partitionValues.asJava)
      .asInstanceOf[Object]

    def getNewWriter(): HiveWriter = {
      val writer = CachedHiveWriters.getOrCreate(
        hiveEndPoint, hiveOptions, hiveOptions.getUGI(), isolatedClassLoader)
      writer.beginTransaction()
      writer
    }
    val writer = inUseWriters.getOrElseUpdate(hiveEndPoint, getNewWriter())

    val jRow = Extraction.decompose(columnName.map { col => col -> row.getAs(col) }.toMap)
    val jString = compact(render(jRow))

    logInfo(s"Write JSON row ${pretty(render(jRow))} into Hive Streaming")
    writer.write(jString.getBytes("UTF-8"))

    if (writer.totalRecords() >= hiveOptions.batchSize) {
      writer.commitTransaction()
      writer.beginTransaction()
    }
  }

  override def abort(): Unit = {
    try {
      inUseWriters.foreach { case (_, writer) =>
        writer.abortTransaction()
        CachedHiveWriters.recycle(writer)
      }
      inUseWriters.clear()
      executorService.shutdown()
    } finally {
      Thread.currentThread().setContextClassLoader(initClassLoader)
    }
  }

  override def commit(): WriterCommitMessage = {
    try {
      inUseWriters.foreach { case (_, writer) =>
        writer.commitTransaction()
        CachedHiveWriters.recycle(writer)
      }
      inUseWriters.clear()
      executorService.shutdown()

      HiveStreamWriterCommitMessage
    } finally {
      Thread.currentThread().setContextClassLoader(initClassLoader)
    }
  }
}