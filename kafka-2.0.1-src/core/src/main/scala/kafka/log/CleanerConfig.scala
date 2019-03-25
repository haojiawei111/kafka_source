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

/**
 * 日志清理程序的配置参数
 * 
 * @param numThreads 要运行的清理线程线程数
 * @param dedupeBufferSize 用于删除日志重复数据使用的总内存
 * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer重复数据删除缓冲区的最大完整百分比
 * @param maxMessageSize The maximum size of a message that can appear in the log可以显示在日志中的消息的最大大小
 * @param maxIoBytesPerSecond The maximum read and write I/O that all cleaner threads are allowed to do允许所有清理线程执行的最大读取和写入I / O.
 * @param backOffMs The amount of time to wait before rechecking if no logs are eligible for cleaning如果没有日志符合清理条件，则在重新检查之前等待的时间
 * @param enableCleaner Allows completely disabling the log cleaner允许完全禁用日志清理程序
 * @param hashAlgorithm The hash algorithm to use in key comparison.用于密钥比较的哈希算法。
 */
case class CleanerConfig(numThreads: Int = 1,
                         dedupeBufferSize: Long = 4*1024*1024L,
                         dedupeBufferLoadFactor: Double = 0.9d,
                         ioBufferSize: Int = 1024*1024,
                         maxMessageSize: Int = 32*1024*1024,
                         maxIoBytesPerSecond: Double = Double.MaxValue,
                         backOffMs: Long = 15 * 1000,
                         enableCleaner: Boolean = true,
                         hashAlgorithm: String = "MD5") {
}
