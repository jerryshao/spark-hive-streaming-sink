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

import java.util.concurrent.{Executors, TimeUnit}
import javax.annotation.Nullable

import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.security.UserGroupInformation

import com.hortonworks.spark.hive.utils.Logging

case class CachedKey(metastoreUri: String, db: String, table: String, partitionCols: Seq[String])

object CachedHiveWriters extends Logging {

  private val cacheExpireTimeout: Long = TimeUnit.MINUTES.toMillis(10)

  private val cache = new mutable.HashMap[CachedKey, mutable.Queue[HiveWriter]]()

  private val executorService = Executors.newSingleThreadScheduledExecutor()
  executorService.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      expireOldestWriters()
    }
  }, 10L, 10L, TimeUnit.MINUTES)

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      try {
        clear()
        executorService.shutdown()
      } catch {
        case NonFatal(_) => // swallow exceptions
      }
    }
  })

  def getOrCreate(
      key: CachedKey,
      hiveEndPoint: Object,
      hiveOptions: HiveOptions,
      @Nullable ugi: UserGroupInformation,
      isolatedClassLoader: ClassLoader): HiveWriter = {
    val writer = CachedHiveWriters.synchronized {
      val queue = cache.getOrElseUpdate(key, new mutable.Queue[HiveWriter]())
      if (queue.isEmpty) {
        None
      } else {
        logDebug(s"Found writer for $key in global cache")
        Some(queue.dequeue())
      }
    }

    writer.getOrElse(new HiveWriter(key, hiveEndPoint, hiveOptions, ugi, isolatedClassLoader))
  }

  def recycle(hiveWriter: HiveWriter): Unit = {
    CachedHiveWriters.synchronized {
      cache.getOrElseUpdate(hiveWriter.key, new mutable.Queue[HiveWriter]())
        .enqueue(hiveWriter)
    }
  }

  private def expireOldestWriters(): Unit = {
    val currentTime = System.currentTimeMillis()
    val expiredWriters = new mutable.ArrayBuffer[HiveWriter]()

    CachedHiveWriters.synchronized {
      val emptyKeys = cache.filter { case (_, queue) =>
        while (queue.nonEmpty) {
          if (queue.head.lastUsed() + cacheExpireTimeout < currentTime) {
            expiredWriters.append(queue.dequeue())
          }
        }
        queue.isEmpty
      }.keySet

      emptyKeys.foreach { k => cache.remove(k) }
    }

    expiredWriters.foreach { w =>
      if (Try { w.close() }.isFailure) {
        logWarn("Failed to close writer")
      } else {
        logInfo(s"Closed expired writer $w")
      }
    }
  }

  private def clear(): Unit = {
    val unusedWriters = new mutable.ArrayBuffer[HiveWriter]()

    CachedHiveWriters.synchronized {
      cache.foreach { case (_, queue) =>
        queue.foreach(unusedWriters.append(_))
      }
      cache.clear()
    }

    unusedWriters.foreach { w =>
      if (Try { w.close() }.isFailure) {
        logWarn("Failed to close writer")
      } else {
        logInfo(s"Closed writer $w")
      }
    }
  }
}