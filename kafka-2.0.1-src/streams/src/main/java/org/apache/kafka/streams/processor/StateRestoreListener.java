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

package org.apache.kafka.streams.processor;

import org.apache.kafka.common.TopicPartition;

/**
 * Class for listening to various states of the restoration process of a StateStore.
 * 用于监听StateStore恢复过程的各种状态的类。
 *
 * When calling {@link org.apache.kafka.streams.KafkaStreams#setGlobalStateRestoreListener(StateRestoreListener)}
 * the passed instance is expected to be stateless since the {@code StateRestoreListener} is shared
 * across all {@link org.apache.kafka.streams.processor.internals.StreamThread} instances.
 *
 * Users desiring stateful operations will need to provide synchronization internally in
 * the {@code StateRestorerListener} implementation.
 *
 * When used for monitoring a single {@link StateStore} using either {@link AbstractNotifyingRestoreCallback} or
 * {@link AbstractNotifyingBatchingRestoreCallback} no synchronization is necessary
 * as each StreamThread has its own StateStore instance.
 *
 * Incremental updates are exposed so users can estimate how much progress has been made.
 * 显示增量更新，以便用户可以估计已经取得了多少进展。
 */
public interface StateRestoreListener {

    /**
     * Method called at the very beginning of {@link StateStore} restoration.
     * 在{@link StateStore}恢复的最开始调用的方法。
     *
     * @param topicPartition the TopicPartition containing the values to restore
     * @param storeName      the name of the store undergoing restoration
     * @param startingOffset the starting offset of the entire restoration process for this TopicPartition
     * @param endingOffset   the exclusive ending offset of the entire restoration process for this TopicPartition
     */
    void onRestoreStart(final TopicPartition topicPartition,
                        final String storeName,
                        final long startingOffset,
                        final long endingOffset);

    /**
     * Method called after restoring a batch of records.  In this case the maximum size of the batch is whatever
     * the value of the MAX_POLL_RECORDS is set to.
     *
     * This method is called after restoring each batch and it is advised to keep processing to a minimum.
     * Any heavy processing will hold up recovering the next batch, hence slowing down the restore process as a
     * whole.
     *
     * If you need to do any extended processing or connecting to an external service consider doing so asynchronously.
     * 恢复一批记录后调用的方法。在这种情况下，批处理的最大大小是MAX_POLL_RECORDS的值设置为*。 *
     * 在恢复每个批次后调用此方法，建议将处理保持在最低限度。
     * 任何繁重的处理都会阻碍恢复下一批，从而减缓恢复过程的整体速度。
     * 如果您需要进行任何扩展处理或连接到外部服务，请考虑异步执行此操作。
     *
     * @param topicPartition the TopicPartition containing the values to restore
     * @param storeName the name of the store undergoing restoration
     * @param batchEndOffset the inclusive ending offset for the current restored batch for this TopicPartition
     * @param numRestored the total number of records restored in this batch for this TopicPartition
     */
    void onBatchRestored(final TopicPartition topicPartition,
                         final String storeName,
                         final long batchEndOffset,
                         final long numRestored);

    /**
     * Method called when restoring the {@link StateStore} is complete.
     * 恢复{@link StateStore}时调用的方法已完成。
     *
     * @param topicPartition the TopicPartition containing the values to restore
     * @param storeName the name of the store just restored
     * @param totalRestored the total number of records restored for this TopicPartition
     */
    void onRestoreEnd(final TopicPartition topicPartition,
                      final String storeName,
                      final long totalRestored);

}
