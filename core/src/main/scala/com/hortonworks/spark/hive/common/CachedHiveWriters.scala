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
import scala.util.control.NonFatal

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.hcatalog.streaming.HiveEndPoint

import com.hortonworks.spark.hive.utils.Logging

object CachedHiveWriters extends Logging {

  private val cacheExpireTimeout: Long = TimeUnit.MINUTES.toMillis(10)

  private val cache = new mutable.HashMap[HiveEndPoint, mutable.Queue[HiveWriter]]()

  private val executorService = Executors.newSingleThreadScheduledExecutor()
  executorService.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      try {
        expireOldestWriters()
      } catch {
        case NonFatal(e) => logWarn("Fail to expire oldest writers", e)
      }
    }
  }, 10L, 10L, TimeUnit.MINUTES)

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      try {
        clear()
        executorService.shutdown()
      } catch {
        case NonFatal(e) => logWarn("Fail to clear all writers", e)
      }
    }
  })

  def getOrCreate(
      hiveEndPoint: HiveEndPoint,
      hiveOptions: HiveOptions,
      @Nullable ugi: UserGroupInformation): HiveWriter = {
    val writer = cache.synchronized {
      val queue = cache.getOrElseUpdate(hiveEndPoint, new mutable.Queue[HiveWriter]())
      if (queue.isEmpty) {
        None
      } else {
        Some(queue.dequeue())
      }
    }

    writer.getOrElse(new HiveWriter(hiveEndPoint, hiveOptions, ugi))
  }

  def recycle(hiveWriter: HiveWriter): Unit = {
    cache.synchronized {
      cache.getOrElseUpdate(hiveWriter.hiveEndPoint, new mutable.Queue[HiveWriter]())
        .enqueue(hiveWriter)
    }
  }

  private def expireOldestWriters(): Unit = {
    val currentTime = System.currentTimeMillis()
    val expiredWriters = new mutable.ArrayBuffer[HiveWriter]()

    cache.synchronized {
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

    expiredWriters.foreach { _.close() }
  }

  private def clear(): Unit = {
    val unusedWriters = new mutable.ArrayBuffer[HiveWriter]()

    cache.synchronized {
      cache.foreach { case (_, queue) =>
        queue.foreach(unusedWriters.append(_))
      }
    }

    unusedWriters.foreach(_.close())
  }
}