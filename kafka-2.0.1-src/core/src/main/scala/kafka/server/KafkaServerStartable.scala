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

import java.util.Properties

import kafka.metrics.KafkaMetricsReporter
import kafka.utils.{Exit, Logging, VerifiableProperties}

/**
  * kafka brokerserverr启动主要类
  */
object KafkaServerStartable {
  def fromProps(serverProps: Properties) = {
    // VerifiableProperties 校验输入参数 把Properties类包装了一层
    val reporters = KafkaMetricsReporter.startReporters(new VerifiableProperties(serverProps))
    //KafkaConfig.fromProps创建KafkaConfig对象，初始化配置参数
    new KafkaServerStartable(KafkaConfig.fromProps(serverProps, false), reporters)
  }
}

/**
  * 创建KafkaServer（new KafkaServer(staticServerConfig, kafkaMetricsReporters = reporters)）对象，
  * 调用KafkaServer的startup、shutdown、awaitShutdown、brokerState.newState(newState)方法
  * brokerState.newState(newState)可以改变broker状态
  *
  * @param staticServerConfig
  * @param reporters
  */
class KafkaServerStartable(val staticServerConfig: KafkaConfig, reporters: Seq[KafkaMetricsReporter]) extends Logging {
  private val server = new KafkaServer(staticServerConfig, kafkaMetricsReporters = reporters)

  def this(serverConfig: KafkaConfig) = this(serverConfig, Seq.empty)

  def startup() {
    try server.startup()
    catch {
      case _: Throwable =>
        // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
        fatal("Exiting Kafka.")
        Exit.exit(1)
    }
  }

  /**
    * 注册的钩子函数，jvm退出的时候执行
    */
  def shutdown() {
    try server.shutdown()
    catch {
      case _: Throwable =>
        fatal("Halting Kafka.")
        // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
        Exit.halt(1)
    }
  }

  /**
   * Allow setting broker state from the startable. 允许从启动设置broker的状态
   * This is needed when a custom kafka server startable want to emit new states that it introduces.
   */
  def setServerState(newState: Byte) {
    server.brokerState.newState(newState)
  }

  def awaitShutdown(): Unit = server.awaitShutdown()

}


