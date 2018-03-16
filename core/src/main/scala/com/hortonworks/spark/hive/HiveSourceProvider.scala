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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import com.hortonworks.spark.hive.common.HiveOptions

class HiveSourceProvider extends DataSourceV2 with StreamWriteSupport with DataSourceRegister {

  override def shortName(): String = "hive-streaming"

  override def createStreamWriter(
      queryId: String,
      schema: StructType,
      outputMode: OutputMode,
      dataSourceOptions: DataSourceOptions): StreamWriter = {
    val localHiveOptions = HiveOptions.fromDataSourceOptions(dataSourceOptions)
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    require(session.isDefined)

    if (outputMode != OutputMode.Append()) {
      throw new IllegalStateException("Hive Streaming only supports output with Append mode")
    }

    val destTable = try {
      session.get.sharedState.externalCatalog.getTable(
        localHiveOptions.dbName, localHiveOptions.tableName)
    } catch {
      case e: Exception => throw new IllegalStateException("Cannot find destination table in " +
        "metastore, please create table at first", e)
    }
    val destSchema = destTable.schema

    if (schema.map(_.name).toSet != destSchema.map(_.name).toSet) {
      throw new IllegalStateException(s"Schema $schema transformed from input source is different" +
        s" from schema $destSchema for the destination table")
    }

    new HiveStreamWriter(schema.map(_.name), destTable.partitionColumnNames, dataSourceOptions)
  }
}
