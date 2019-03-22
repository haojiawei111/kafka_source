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

/**
 * Broker states are the possible state that a kafka broker can be in.
 * A broker should be only in one state at a time.
 * The expected state transition with the following defined states is:
  * 经纪人状态是卡夫卡经纪人可以参与的可能状态。经纪人应该每次只能处于一种状态。具有以下定义状态的期望状态转换为：
 *
 *                +-----------+
 *                |Not Running|
 *                +-----+-----+
 *                      |
 *                      v
 *                +-----+-----+
 *                |Starting   +--+
 *                +-----+-----+  | +----+------------+
 *                      |        +>+RecoveringFrom   |
 *                      v          |UncleanShutdown  |
 *               +-------+-------+ +-------+---------+
 *               |RunningAsBroker|            |
 *               +-------+-------+<-----------+
 *                       |
 *                       v
 *                +-----+------------+
 *                |PendingControlled |
 *                |Shutdown          |
 *                +-----+------------+
 *                      |
 *                      v
 *               +-----+----------+
 *               |BrokerShutting  |
 *               |Down            |
 *               +-----+----------+
 *                     |
 *                     v
 *               +-----+-----+
 *               |Not Running|
 *               +-----------+
 *
 * Custom states is also allowed for cases where there are custom kafka states for different scenarios.
 */
/**
  * 其修饰的trait，class只能在当前文件里面被继承
  * 用sealed修饰这样做的目的是告诉scala编译器在检查模式匹配的时候，
  * 让scala知道这些case的所有情况，scala就能够在编译的时候进行检查，看你写的代码是否有没有漏掉什么没case到，减少编程的错误。
  */
sealed trait BrokerStates { def state: Byte }
case object NotRunning extends BrokerStates { val state: Byte = 0 }
case object Starting extends BrokerStates { val state: Byte = 1 }
case object RecoveringFromUncleanShutdown extends BrokerStates { val state: Byte = 2 }
case object RunningAsBroker extends BrokerStates { val state: Byte = 3 }
case object PendingControlledShutdown extends BrokerStates { val state: Byte = 6 }
case object BrokerShuttingDown extends BrokerStates { val state: Byte = 7 }


case class BrokerState() {
  @volatile var currentState: Byte = NotRunning.state

  def newState(newState: BrokerStates) {
    this.newState(newState.state)
  }

  // Allowing undefined custom state 允许未定义的自定义状态
  def newState(newState: Byte) {
    currentState = newState
  }
}
