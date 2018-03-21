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

import java.util.concurrent.TimeUnit

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.hcatalog.streaming._

import com.hortonworks.spark.hive.utils.Logging

class HiveWriter(
    val hiveEndPoint: HiveEndPoint,
    hiveOptions: HiveOptions,
    ugi: UserGroupInformation) extends Logging {

  private val hiveConf = new HiveConf
  private val txnTimeout =
    hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS)

  private val connection =
    hiveEndPoint.newConnection(hiveOptions.autoCreatePartitions, hiveConf, ugi)

  // TODO. for now we only support to write JSON String to Hive Streaming.
  private val writer = new StrictJsonWriter(hiveEndPoint, hiveConf)

  private var txnBatch: TransactionBatch = null

  // Timestamp to track the activity of this HiveWriter
  private var _lastUsed: Long = System.currentTimeMillis()

  // Timestamp to track the last creation time of transaction batch
  private var _lastCreated = System.currentTimeMillis()

  // Track the number of records written in this batch
  private var _totalRecords = 0

  private var isTransactionBegin = false

  def beginTransaction(): Unit = {
    if (txnBatch != null && txnBatch.remainingTransactions() == 0) {
      txnBatch.close()
      txnBatch = null
    }

    if (txnBatch == null) {
      txnBatch = connection.fetchTransactionBatch(hiveOptions.txnPerBatch, writer)
      _lastCreated = System.currentTimeMillis()
    }

    txnBatch.beginNextTransaction()
    isTransactionBegin = true
    _totalRecords = 0

    logDebug(s"Switch to next transaction for $hiveEndPoint")
  }

  def write(record: Array[Byte]): Unit = {
    require(txnBatch != null, "current transaction is not initialized before writing")
    require(isTransactionBegin, "current transaction is not beginning")

    txnBatch.write(record)
    _totalRecords += 1
  }

  def commitTransaction(): Unit = {
    require(txnBatch != null, "current transaction is not initialized before committing")
    require(isTransactionBegin, "current transaction is not beginning")

    txnBatch.commit()

    _lastUsed = System.currentTimeMillis()
    isTransactionBegin = false
    _totalRecords = 0
  }

  def abortTransaction(): Unit = {
    isTransactionBegin = false
    _totalRecords = 0

    if (txnBatch != null) {
      txnBatch.abort()
    }
  }

  def close(): Unit = {
    isTransactionBegin = false
    _totalRecords = 0

    if (txnBatch != null) {
      txnBatch.commit()
      txnBatch.close()
    }

    connection.close()
  }

  def lastUsed(): Long = _lastUsed

  def totalRecords(): Int = _totalRecords

  def heartbeat(): Unit = {
    if (System.currentTimeMillis() - _lastCreated > txnTimeout / 2) {
      if (txnBatch != null) {
        txnBatch.heartbeat()
      }
    }
  }
}
