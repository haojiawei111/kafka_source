/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.server.QuotaType._
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.server.quota.ClientQuotaCallback
import org.apache.kafka.common.utils.Time

// 配额类型
object QuotaType  {
  case object Fetch extends QuotaType   //拉数据
  case object Produce extends QuotaType //生产
  case object Request extends QuotaType  //请求
  case object LeaderReplication extends QuotaType  //领导者复制
  case object FollowerReplication extends QuotaType //跟随着复制
  case object AlterLogDirsReplication extends QuotaType //更改日志目录复制
}

sealed trait QuotaType

//配额管理类工厂
object QuotaFactory extends Logging {

  object UnboundedQuota extends ReplicaQuota {
    override def isThrottled(topicPartition: TopicPartition): Boolean = false
    override def isQuotaExceeded: Boolean = false
    def record(value: Long): Unit = ()
  }

  // 样例类
  case class QuotaManagers(fetch: ClientQuotaManager,
                           produce: ClientQuotaManager,
                           request: ClientRequestQuotaManager,
                           leader: ReplicationQuotaManager,
                           follower: ReplicationQuotaManager,
                           alterLogDirs: ReplicationQuotaManager,
                           clientQuotaCallback: Option[ClientQuotaCallback]) {

    def shutdown() {
      fetch.shutdown
      produce.shutdown
      request.shutdown
      clientQuotaCallback.foreach(_.close())
    }

  }

  // 拿到QuotaManagers实例
  def instantiate(cfg: KafkaConfig, metrics: Metrics, time: Time, threadNamePrefix: String): QuotaManagers = {

    //clientQuotaCallback 没有配client.quota.callback.class ，默认为空， 后面的代码会设置默认的配额回调类 DefaultQuotaCallback
    val clientQuotaCallback = Option(cfg.getConfiguredInstance(KafkaConfig.ClientQuotaCallbackClassProp, //client.quota.callback.class
      classOf[ClientQuotaCallback]))

    // 这里每个QuotaManager都会启动一个线程
    QuotaManagers(
      // Fetch
      new ClientQuotaManager(clientFetchConfig(cfg), metrics, Fetch, time, threadNamePrefix, clientQuotaCallback),
      //Produce
      new ClientQuotaManager(clientProduceConfig(cfg), metrics, Produce, time, threadNamePrefix, clientQuotaCallback),
      // request
      new ClientRequestQuotaManager(clientRequestConfig(cfg), metrics, time, threadNamePrefix, clientQuotaCallback),
      //LeaderReplication
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, LeaderReplication, time),
      //FollowerReplication
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, FollowerReplication, time),
      //AlterLogDirsReplication
      new ReplicationQuotaManager(alterLogDirsReplicationConfig(cfg), metrics, AlterLogDirsReplication, time),

      clientQuotaCallback
    )
  }

  // 解析配置文件 配置producer配额管理
  def clientProduceConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    //quota.producer.default
    if (cfg.producerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.")
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault, //quota.producer.default
      numQuotaSamples = cfg.numQuotaSamples, //quota.window.num
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds   //quota.window.size.seconds
    )
  }

  // 解析配置文件 配置consumer配额管理
  def clientFetchConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    //quota.consumer.default
    if (cfg.consumerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.")
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault, //quota.consumer.default
      numQuotaSamples = cfg.numQuotaSamples,  //quota.window.num
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds    //quota.window.size.seconds
    )
  }

  // 解析配置文件 配置Request配额管理
  def clientRequestConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    ClientQuotaManagerConfig(
      numQuotaSamples = cfg.numQuotaSamples,    //quota.window.num
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds     //quota.window.size.seconds
    )
  }

  // 解析配置文件 配置replica配额管理
  def replicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numReplicationQuotaSamples,  //replication.quota.window.num
      quotaWindowSizeSeconds = cfg.replicationQuotaWindowSizeSeconds  //replication.quota.window.size.seconds
    )
  }

  // 解析配置文件 配置alterLogDirsReplication配额管理
  def alterLogDirsReplicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numAlterLogDirsReplicationQuotaSamples,  //alter.log.dirs.replication.quota.window.num
      quotaWindowSizeSeconds = cfg.alterLogDirsReplicationQuotaWindowSizeSeconds //alter.log.dirs.replication.quota.window.size.seconds
    )
  }

}
