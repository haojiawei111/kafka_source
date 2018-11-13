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
package org.apache.kafka.server.policy;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An interface for enforcing a policy on create topics requests.一种用于执行关于创建主题请求的策略的接口。
 *
 * Common use cases are requiring that the replication factor, min.insync.replicas and/or retention settings for a
 * topic are within an allowable range.
 *
 * If <code>create.topic.policy.class.name</code> is defined, Kafka will create an instance of the specified class
 * using the default constructor and will then pass the broker configs to its <code>configure()</code> method. During
 * broker shutdown, the <code>close()</code> method will be invoked so that resources can be released (if necessary).
 */
public interface CreateTopicPolicy extends Configurable, AutoCloseable {

    /**
     * Class containing the create request parameters.
     */
    class RequestMetadata {
        private final String topic;
        private final Integer numPartitions;
        private final Short replicationFactor;
        private final Map<Integer, List<Integer>> replicasAssignments;
        private final Map<String, String> configs;

        /**
         * Create an instance of this class with the provided parameters.
         *
         * This constructor is public to make testing of <code>CreateTopicPolicy</code> implementations easier.
         *
         * @param topic the name of the topic to created.
         * @param numPartitions the number of partitions to create or null if replicasAssignments is set.
         * @param replicationFactor the replication factor for the topic or null if replicaAssignments is set.
         * @param replicasAssignments replica assignments or null if numPartitions and replicationFactor is set. The
         *                            assignment is a map from partition id to replica (broker) ids.
         *                            如果设置了数字分区和复制因子，则复制分配或NULL。分配是从分区ID到副本（代理）ID的映射
         * 。/kafka-topics.sh --zookeeper ZooKeeperHost:ZooKeeperPort --create --topic TopicName
         *                            --replica-assignment id0:id1:id2,id3:id4:id5,id6:id7:id8
         * 其中，“id0:id1:id2,id3:id4:id5,id6:id7:id8”表示Topic TopicName一共有3个Partition（以“,”分隔），每个Partition均有3个Replica（以“:”分隔）
         * @param configs topic configs for the topic to be created, not including broker defaults. Broker configs are
         *                passed via the {@code configure()} method of the policy implementation.
         */
        public RequestMetadata(String topic, Integer numPartitions, Short replicationFactor,
                        Map<Integer, List<Integer>> replicasAssignments, Map<String, String> configs) {
            this.topic = topic;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.replicasAssignments = replicasAssignments == null ? null : Collections.unmodifiableMap(replicasAssignments);
            this.configs = Collections.unmodifiableMap(configs);
        }

        /**
         * Return the name of the topic to create.
         */
        public String topic() {
            return topic;
        }

        /**
         * Return the number of partitions to create or null if replicaAssignments is not null.
         */
        public Integer numPartitions() {
            return numPartitions;
        }

        /**
         * Return the number of replicas to create or null if replicaAssignments is not null.
         */
        public Short replicationFactor() {
            return replicationFactor;
        }

        /**
         * Return a map from partition id to replica (broker) ids or null if numPartitions and replicationFactor are
         * set instead.
         */
        public Map<Integer, List<Integer>> replicasAssignments() {
            return replicasAssignments;
        }

        /**
         * Return topic configs in the request, not including broker defaults. Broker configs are passed via
         * the {@code configure()} method of the policy implementation.
         */
        public Map<String, String> configs() {
            return configs;
        }

        @Override
        public String toString() {
            return "CreateTopicPolicy.RequestMetadata(topic=" + topic +
                    ", numPartitions=" + numPartitions +
                    ", replicationFactor=" + replicationFactor +
                    ", replicasAssignments=" + replicasAssignments +
                    ", configs=" + configs + ")";
        }
    }

    /**
     * Validate the request parameters and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the create topics request parameters for the provided topic do not satisfy this policy.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message. Note that validation
     * failure only affects the relevant topic, other topics in the request will still be processed.
     *
     * @param requestMetadata the create topics request parameters for the provided topic.
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validate(RequestMetadata requestMetadata) throws PolicyViolationException;
}
