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

import java.util.{Map => JMap}
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.hive.hcatalog.streaming.HiveEndPoint
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.json4s.{DefaultFormats, Extraction}
import org.json4s.jackson.JsonMethods._
import com.hortonworks.spark.hive.common.{CachedHiveWriters, HiveOptions, HiveWriter}
import com.hortonworks.spark.hive.utils.Logging

case object HiveStreamWriterCommitMessage extends WriterCommitMessage

class HiveStreamWriter(
    columnNames: Seq[String],
    partitionCols: Seq[String],
    dataSourceOptions: DataSourceOptions)
  extends StreamWriter {

  override def createWriterFactory(): DataWriterFactory[Row] = {
    new HiveStreamDataWriterFactory(columnNames, partitionCols, dataSourceOptions.asMap())
  }

  override def commit(epochId: Long, writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class HiveStreamDataWriterFactory(
    columnName: Seq[String],
    partitionCols: Seq[String],
    dataSourceOptionsMap: JMap[String, String]) extends DataWriterFactory[Row] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new HiveStreamDataWriter(
      partitionId,
      attemptNumber,
      columnName,
      partitionCols,
      dataSourceOptionsMap)
  }
}

class HiveStreamDataWriter(
    partitionId: Int,
    attemptNumber: Int,
    columnName: Seq[String],
    partitionCols: Seq[String],
    dataSourceOptionsMap: JMap[String, String]) extends DataWriter[Row] with Logging {

  private implicit def formats = DefaultFormats

  private val hiveOptions =
    HiveOptions.fromDataSourceOptions(new DataSourceOptions(dataSourceOptionsMap))

  private val inUseWriters = new mutable.HashMap[HiveEndPoint, HiveWriter]()

  private val executorService = Executors.newSingleThreadScheduledExecutor()
  executorService.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      inUseWriters.foreach(_._2.heartbeat())
    }
  }, 10L, 10L, TimeUnit.SECONDS)

  override def write(row: Row): Unit = {
    val partitionValues = partitionCols.map { col => row.getAs[String](col) }
    val hiveEndPoint = new HiveEndPoint(
      hiveOptions.metastoreUri, hiveOptions.dbName, hiveOptions.tableName, partitionValues.asJava)

    def getNewWriter(): HiveWriter = {
      val writer = CachedHiveWriters.getOrCreate(
        hiveEndPoint, hiveOptions, hiveOptions.getUGI())
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
    inUseWriters.foreach { case (_, writer) =>
      writer.abortTransaction()
      CachedHiveWriters.recycle(writer)
    }
    inUseWriters.clear()
    executorService.shutdown()
  }

  override def commit(): WriterCommitMessage = {
    inUseWriters.foreach { case (_, writer) =>
      writer.commitTransaction()
        CachedHiveWriters.recycle(writer)
    }
    inUseWriters.clear()
    executorService.shutdown()

    HiveStreamWriterCommitMessage
  }
}
