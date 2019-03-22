package kafka.common

import org.apache.kafka.common.TopicPartition


/**
 * Convenience case class since (topic, partition) pairs are ubiquitous.
  *
 */
case class TopicAndPartition(topic: String, partition: Int) {

  def this(topicPartition: TopicPartition) = this(topicPartition.topic, topicPartition.partition)

  override def toString: String = s"$topic-$partition"
}
