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

package com.hortonworks.spark.hive.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 * A Hive Streaming example to ingest data from rate stream and push into hive table.
 *
 * Assumed HIVE table Schema:
 * create table rate (value bigint)
 *    clustered by (value) into 5 buckets
 *    stored as orc tblproperties("transactional"="true");
 */
object RateStreamingExample {

  def main(args: Array[String]): Unit = {
    if (args.length < 1 || args.length > 4) {
      // scalastyle:off println
      System.err.println(s"Usage: RateStreamingExample <metastore uri> [principal] " +
        s"[keytab] [continuous?]")
      // scalastyle:on println
      System.exit(1)
    }

    val metastoreUri = args(0)
    val continuous = if (args.length == 2) {
      args(1) == "continuous"
    } else if (args.length == 4) {
      args(3) == "continuous"
    } else {
      false
    }

    val principal = if (args.length >= 3) args(1) else null
    val keytab = if (args.length >= 3) args(2) else null

    val sparkConf = new SparkConf()
      .set("spark.sql.streaming.checkpointLocation", "./checkpoint")
    val sparkSession = SparkSession.builder()
      .appName("RateStreamingExample")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val rate = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load()

    val writer = rate.select("value")
      .writeStream
      .format("hive-streaming")
      .option("metastore", metastoreUri)
      .option("db", "default")
      .option("table", "rate")

    if (principal != null && keytab != null) {
      writer.option("principal", principal)
      writer.option("keytab", keytab)
    }

    val query = writer
      .trigger(if (continuous) Trigger.Continuous(3000L) else Trigger.ProcessingTime(3000L))
      .start()

    query.awaitTermination()

    query.stop()
    sparkSession.stop()
  }
}
