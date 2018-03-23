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

package com.hortonworks.spark.hive.common

import org.apache.hadoop.security.UserGroupInformation

import com.hortonworks.spark.hive.utils.Logging

class HiveWriter(
    val hiveEndPoint: Object,
    hiveOptions: HiveOptions,
    ugi: UserGroupInformation,
    isolatedClassLoader: ClassLoader) extends Logging {

  private val hiveConf =
    Class.forName("org.apache.hadoop.hive.conf.HiveConf", true, isolatedClassLoader)
    .newInstance()
    .asInstanceOf[Object]
  private val txnTimeout = 300 * 1000L

  private val connection = hiveEndPoint.getClass.getMethod(
    "newConnection",
    classOf[Boolean],
    Class.forName("org.apache.hadoop.hive.conf.HiveConf", true, isolatedClassLoader),
    classOf[UserGroupInformation])
    .invoke(hiveEndPoint, hiveOptions.autoCreatePartitions: java.lang.Boolean, hiveConf, ugi)

  // TODO. for now we only support to write JSON String to Hive Streaming.
  private val writer =
    Class.forName("org.apache.hive.hcatalog.streaming.StrictJsonWriter", true, isolatedClassLoader)
    .getConstructor(
      Class.forName("org.apache.hive.hcatalog.streaming.HiveEndPoint", true, isolatedClassLoader),
      Class.forName("org.apache.hadoop.hive.conf.HiveConf", true, isolatedClassLoader))
    .newInstance(hiveEndPoint, hiveConf)
    .asInstanceOf[Object]

  private var txnBatch: Object = null

  // Timestamp to track the activity of this HiveWriter
  private var _lastUsed: Long = System.currentTimeMillis()

  // Timestamp to track the last creation time of transaction batch
  private var _lastCreated = System.currentTimeMillis()

  // Track the number of records written in this batch
  private var _totalRecords = 0

  private var isTransactionBegin = false

  def beginTransaction(): Unit = {
    if (txnBatch != null && call[Int](txnBatch, "remainingTransactins") == 0) {
      call[Unit](txnBatch, "close")
      txnBatch = null
    }

    if (txnBatch == null) {
      txnBatch = call[Object](connection, "fetchTransactionBatch", Seq(classOf[Int],
        Class.forName(
          "org.apache.hive.hcatalog.streaming.RecordWriter", true, isolatedClassLoader)),
        Seq(hiveOptions.txnPerBatch: java.lang.Integer, writer))
      _lastCreated = System.currentTimeMillis()
    }

    call[Unit](txnBatch, "beginNextTransaction")
    isTransactionBegin = true
    _totalRecords = 0

    logDebug(s"Switch to next transaction for $hiveEndPoint")
  }

  def write(record: Array[Byte]): Unit = {
    require(txnBatch != null, "current transaction is not initialized before writing")
    require(isTransactionBegin, "current transaction is not beginning")

    call[Unit](txnBatch, "write", Seq(classOf[Array[Byte]]), Seq(record))
    _totalRecords += 1
  }

  def commitTransaction(): Unit = {
    require(txnBatch != null, "current transaction is not initialized before committing")
    require(isTransactionBegin, "current transaction is not beginning")

    call[Unit](txnBatch, "commit")

    _lastUsed = System.currentTimeMillis()
    isTransactionBegin = false
    _totalRecords = 0
  }

  def abortTransaction(): Unit = {
    isTransactionBegin = false
    _totalRecords = 0

    if (txnBatch != null) {
      call[Unit](txnBatch, "abort")
    }
  }

  def close(): Unit = {
    isTransactionBegin = false
    _totalRecords = 0

    if (txnBatch != null) {
      call[Unit](txnBatch, "commit")
      call[Unit](txnBatch, "close")
    }

    call[Unit](connection, "close")
  }

  def lastUsed(): Long = _lastUsed

  def totalRecords(): Int = _totalRecords

  def heartbeat(): Unit = {
    if (System.currentTimeMillis() - _lastCreated > txnTimeout / 2) {
      if (txnBatch != null) {
        call[Unit](txnBatch, "heartbeat")
      }
    }
  }

  private def call[T](
      obj: Object,
      method: String,
      types: Seq[Class[_]] = Seq.empty,
      params: Seq[Object] = Seq.empty): T = {
    val mtd = obj.getClass.getMethod(method, types: _*)
    mtd.setAccessible(true)
    mtd.invoke(obj, params: _*).asInstanceOf[T]
  }
}
