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

import java.io.File
import javax.annotation.Nullable

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.sources.v2.DataSourceOptions

import com.hortonworks.spark.hive.utils.Logging

class HiveOptions private (
    val metastoreUri: String,
    val dbName: String,
    val tableName: String) extends Logging {

  var txnPerBatch = 100
  var batchSize = 10000
  var autoCreatePartitions = true

  private var principal: String = null
  private var keytab: String = null

  def withTxnPerBatch(txnPerBatch: Int): HiveOptions = {
    this.txnPerBatch = txnPerBatch
    this
  }

  def withAutoCreatePartitions(autoCreatePartitions: Boolean): HiveOptions = {
    this.autoCreatePartitions = autoCreatePartitions
    this
  }

  def withPrincipalAndKeytab(principal: String, keytab: String): HiveOptions = {
    this.principal = principal
    this.keytab = keytab
    this
  }

  def withBatchSize(batchSize: Int): HiveOptions = {
    this.batchSize = batchSize
    this
  }

  @Nullable
  def getUGI(): UserGroupInformation = {
    if (principal == null || keytab == null) {
      null.asInstanceOf[UserGroupInformation]
    } else {
      val kfile = new File(keytab)
      if (!(kfile.exists && kfile.canRead)) {
        throw new IllegalArgumentException(s"keytab file $keytab is not existed or unreadable")
      }

      val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
      logInfo(s"UGI $ugi with princial $principal and keytab $keytab")
      ugi
    }
  }
}

object HiveOptions {
  // Key of hive options (case insensitive).
  val METASTORE_URI = "metastore"
  val DB_NAME = "db"
  val TABLE_NAME = "table"
  val TXN_PER_BATCH = "txn.per.batch"
  val AUTO_CREATE_PARTITIONS = "auto.create.partitions"
  val PRINCIPAL = "principal"
  val KEYTAB = "keytab"
  val BATCH_SIZE = "batch.size"

  def fromDataSourceOptions(dataSourceOptions: DataSourceOptions): HiveOptions = {
    val metastoreUri = dataSourceOptions.get(METASTORE_URI)
    if (!metastoreUri.isPresent) {
      throw new IllegalArgumentException("metastore URI must be specified")
    }

    val dbName = dataSourceOptions.get(DB_NAME)
    if (!dbName.isPresent) {
      throw new IllegalArgumentException("db name must be specified")
    }

    val tblName = dataSourceOptions.get(TABLE_NAME)
    if (!tblName.isPresent) {
      throw new IllegalArgumentException("table name must be specified")
    }

    val option = new HiveOptions(metastoreUri.get(), dbName.get(), tblName.get())

    option.withTxnPerBatch(dataSourceOptions.getInt(TXN_PER_BATCH, option.txnPerBatch))
      .withAutoCreatePartitions(
        dataSourceOptions.getBoolean(AUTO_CREATE_PARTITIONS, option.autoCreatePartitions))
      .withPrincipalAndKeytab(
        dataSourceOptions.get(PRINCIPAL).orElse(option.principal),
        dataSourceOptions.get(KEYTAB).orElse(option.keytab))
      .withBatchSize(dataSourceOptions.getInt(BATCH_SIZE, option.batchSize))
  }
}
