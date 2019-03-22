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

package kafka.utils

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.kafka.common.internals.FatalExitError

abstract class ShutdownableThread(val name: String, val isInterruptible: Boolean = true)extends Thread(name) with Logging {

  this.setDaemon(false)
  this.logIdent = "[" + name + "]: "
  private val shutdownInitiated = new CountDownLatch(1)
  private val shutdownComplete = new CountDownLatch(1)



  //shutdownComplete 等于 0 则都关闭了
  def isShutdownComplete: Boolean = {
    shutdownComplete.getCount == 0
  }

  // 关闭线程
  def shutdown(): Unit = {
    // 执行关闭
    initiateShutdown()
    //等待关闭
    awaitShutdown()
  }

  // 关闭线程
  def initiateShutdown(): Boolean = {
    this.synchronized {
      if (isRunning) {
        info("Shutting down")
        shutdownInitiated.countDown()
        if (isInterruptible)
          interrupt()
        true
      } else
        false
    }
  }

  /**
    * 在调用initiateShutdown（）之后，使用此API等待关闭完成
   */
  def awaitShutdown(): Unit = {
    shutdownComplete.await()
    info("Shutdown completed")
  }

  /**
    *  导致当前线程等待，直到启动关闭，或指定的等待时间过去
   *
   * @param timeout
   * @param unit
   */
  def pause(timeout: Long, unit: TimeUnit): Unit = {
    if (shutdownInitiated.await(timeout, unit))
      trace("shutdownInitiated latch count reached zero. Shutdown called.shutdownInitiated锁存计数达到零。关机叫了。")
  }

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
    * 重复调用此方法，直到线程关闭或此方法抛出异常
   */
  def doWork(): Unit

  override def run(): Unit = {
    info("Starting")
    try {
      while (isRunning)
        // 一直无限循环doWork()方法
        doWork()
    } catch {
      case e: FatalExitError =>
        shutdownInitiated.countDown()
        shutdownComplete.countDown()
        info("Stopped")
        Exit.exit(e.statusCode())
      case e: Throwable =>
        if (isRunning)
          error("Error due to", e)
    } finally {
       shutdownComplete.countDown()
    }
    info("Stopped")
  }

  def isRunning: Boolean = {
    // shutdownInitiated不等于0则有线程在运行
    shutdownInitiated.getCount() != 0
  }
}
