/**
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


package kafka.server

import java.io.IOException
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

import kafka.utils.Logging

/*
 * LogDirFailureChannel allows an external thread to block waiting for new offline log dirs.
 * LogDirFailureChannel允许外部线程阻止等待新的脱机日志目录。
 *
 * There should be a single instance of LogDirFailureChannel accessible by any class that does disk-IO operation.
 * If IOException is encountered while accessing a log directory, the corresponding class can add the log directory name
 * to the LogDirFailureChannel using maybeAddOfflineLogDir(). Each log directory will be added only once. After a log
 * directory is added for the first time, a thread which is blocked waiting for new offline log directories
 * can take the name of the new offline log directory out of the LogDirFailureChannel and handle the log failure properly.
 * An offline log directory will stay offline until the broker is restarted.
 * 任何执行disk-IO操作的类都应该可以访问LogDirFailureChannel的单个实例。
 * 如果在访问日志目录时遇到IOException，则相应的类可以使用maybeAddOfflineLogDir（）将日志目录名*添加到LogDirFailureChannel。每个日志目录只会添加一次。在第一次添加log
 * 目录后，阻塞等待新的脱机日志目录*的线程可以将新的脱机日志目录的名称从LogDirFailureChannel中取出并正确处理日志故障。 *脱机日志目录将保持脱机状态，直到重新启动代理。
 *
 */
class LogDirFailureChannel(logDirNum: Int) extends Logging {

  private val offlineLogDirs = new ConcurrentHashMap[String, String]
  private val offlineLogDirQueue = new ArrayBlockingQueue[String](logDirNum)

  /*
   * If the given logDir is not already offline, add it to the
   * set of offline log dirs and enqueue it to the logDirFailureEvent queue
   * 如果给定的logDir尚未脱机，请将其添加到* set of offline log dirs并将其排入logDirFailureEvent队列
   */
  def maybeAddOfflineLogDir(logDir: String, msg: => String, e: IOException): Unit = {
    error(msg, e)
    if (offlineLogDirs.putIfAbsent(logDir, logDir) == null) {
      offlineLogDirQueue.add(logDir)
    }
  }

  /*
   * Get the next offline log dir from logDirFailureEvent queue.
   * The method will wait if necessary until a new offline log directory becomes available
   * 从logDirFailureEvent队列中获取下一个脱机日志目录。 *该方法将在必要时等待，直到新的脱机日志目录可用
   */
  def takeNextOfflineLogDir(): String = offlineLogDirQueue.take()

}
