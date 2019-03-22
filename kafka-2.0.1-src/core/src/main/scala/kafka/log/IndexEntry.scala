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

package kafka.log

import org.apache.kafka.common.requests.ListOffsetResponse

// IndexEntry的接口
sealed trait IndexEntry {
  // We always use Long for both key and value to avoid boxing.
  // 我们总是使用Long作为键和值来避免碰撞。
  def indexKey: Long
  def indexValue: Long
}

/**
  * 逻辑日志偏移量与消息集条目开头的某个日志文件中的物理位置之间的映射，带有给定的偏移量。
 */
case class OffsetPosition(offset: Long, position: Int) extends IndexEntry {
  override def indexKey = offset
  override def indexValue = position.toLong
}


/**
  * 时间戳与消息偏移之间的映射。该条目表示任何时间戳大于该时间戳的消息必须在该偏移量处或之后。
 * @param timestamp The max timestamp before the given offset.
 * @param offset The message offset.
 */
case class TimestampOffset(timestamp: Long, offset: Long) extends IndexEntry {
  override def indexKey = timestamp
  override def indexValue = offset
}

object TimestampOffset {
  // 未知时间戳 身份offset
  val Unknown = TimestampOffset(ListOffsetResponse.UNKNOWN_TIMESTAMP, ListOffsetResponse.UNKNOWN_OFFSET)
}
