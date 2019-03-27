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

package kafka.coordinator.group

import java.util

import kafka.utils.nonthreadsafe
import org.apache.kafka.common.protocol.Errors


case class MemberSummary(memberId: String,
                         clientId: String,
                         clientHost: String,
                         metadata: Array[Byte],
                         assignment: Array[Byte])

/**
 * Member metadata contains the following metadata: 成员元数据包含以下元数据：
 *
 * Heartbeat metadata:心跳元数据：
 * 1. negotiated heartbeat session timeout协商心跳会话超时
 * 2. timestamp of the latest heartbeat最新心跳的时间戳
 *
 * Protocol metadata:协议元数据：
 * 1. the list of supported protocols (ordered by preference)支持的协议列表
 * 2. the metadata associated with each protocol与每个协议相关联的元数据
 *
 * In addition, it also contains the following state information:此外，它还包含以下状态信息：
 *
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
  *                                 等待重新平衡回调：当组处于准备 - 重新平衡状态时，如果成员已发送加入组请求，则其重新平衡回调将保留在元数据中
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
  *                            等待同步回调：当组处于等待同步状态时，其同步回调保留在元数据中，直到领导者提供组分配并且组转换为稳定
 */
@nonthreadsafe
private[group] class MemberMetadata(val memberId: String,
                                    val groupId: String,
                                    val clientId: String,
                                    val clientHost: String,
                                    val rebalanceTimeoutMs: Int,
                                    val sessionTimeoutMs: Int,
                                    val protocolType: String,
                                    var supportedProtocols: List[(String, Array[Byte])]) {

  var assignment: Array[Byte] = Array.empty[Byte]
  var awaitingJoinCallback: JoinGroupResult => Unit = null
  var awaitingSyncCallback: (Array[Byte], Errors) => Unit = null
  var latestHeartbeat: Long = -1
  var isLeaving: Boolean = false

  def protocols = supportedProtocols.map(_._1).toSet

  /**
   * Get metadata corresponding to the provided protocol.
   */
  def metadata(protocol: String): Array[Byte] = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  /**
   * Check if the provided protocol metadata matches the currently stored metadata.
   */
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    if (protocols.size != this.supportedProtocols.size)
      return false

    for (i <- protocols.indices) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    true
  }

  def summary(protocol: String): MemberSummary = {
    MemberSummary(memberId, clientId, clientHost, metadata(protocol), assignment)
  }

  def summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   */
  def vote(candidates: Set[String]): String = {
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      case Some((protocol, _)) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString: String = {
    "MemberMetadata(" +
      s"memberId=$memberId, " +
      s"clientId=$clientId, " +
      s"clientHost=$clientHost, " +
      s"sessionTimeoutMs=$sessionTimeoutMs, " +
      s"rebalanceTimeoutMs=$rebalanceTimeoutMs, " +
      s"supportedProtocols=${supportedProtocols.map(_._1)}, " +
      ")"
  }

}
