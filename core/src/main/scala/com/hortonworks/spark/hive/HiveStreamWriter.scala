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

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter

import com.hortonworks.spark.hive.utils.HiveIsolatedClassLoader

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
    val restoredClassLoader = Thread.currentThread().getContextClassLoader
    val currentClassLoader = HiveIsolatedClassLoader.isolatedClassLoader()
    try {
      Thread.currentThread().setContextClassLoader(currentClassLoader)

      currentClassLoader.loadClass(classOf[HiveStreamDataWriter].getName)
        .getConstructors.head
        .newInstance(partitionId: java.lang.Integer, attemptNumber: java.lang.Integer,
          columnName, partitionCols, dataSourceOptionsMap, restoredClassLoader, currentClassLoader)
        .asInstanceOf[DataWriter[Row]]
    } finally {
      Thread.currentThread().setContextClassLoader(restoredClassLoader)
    }
  }
}
