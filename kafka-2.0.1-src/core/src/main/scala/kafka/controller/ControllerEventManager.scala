/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
}

class ControllerEventManager(controllerId: Int, rateAndTimeMetrics: Map[ControllerState, KafkaTimer],eventProcessedListener: ControllerEvent => Unit) extends KafkaMetricsGroup {

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  // 阻塞队列
  private val queue = new LinkedBlockingQueue[ControllerEvent]
  // 创建controller-event-thread 事件线程
  private val thread = new ControllerEventThread(ControllerEventManager.ControllerEventThreadName)
  private val time = Time.SYSTEM

  private val eventQueueTimeHist = newHistogram("EventQueueTimeMs")

  newGauge(
    "EventQueueSize",
    new Gauge[Int] {
      def value: Int = {
        queue.size()
      }
    }
  )


  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    clearAndPut(KafkaController.ShutdownEventThread)
    thread.awaitShutdown()
  }

  // 清空queue并向queue加入一个event
  def clearAndPut(event: ControllerEvent): Unit = inLock(putLock) {
    queue.clear()
    put(event)
  }


  def put(event: ControllerEvent): Unit = inLock(putLock) {
    queue.put(event)
  }

  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {

    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      // 从队列中拿值
      queue.take() match {
        case KafkaController.ShutdownEventThread => initiateShutdown()
        case controllerEvent =>
          _state = controllerEvent.state

          eventQueueTimeHist.update(time.milliseconds() - controllerEvent.enqueueTimeMs)

          try {
            rateAndTimeMetrics(state).time {
          // 执行事件的程序
          controllerEvent.process()
        }
      } catch {
            case e: Throwable => error(s"Error processing event $controllerEvent", e)
          }
          // 更新监控指标
          try eventProcessedListener(controllerEvent)
          catch {
            case e: Throwable => error(s"Error while invoking listener for processed event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

}
