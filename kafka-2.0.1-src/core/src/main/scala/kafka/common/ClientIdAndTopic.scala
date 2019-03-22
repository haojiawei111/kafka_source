

package kafka.common

/**
 * Convenience case class since (clientId, topic) pairs are used in the creation
 * of many Stats objects.
 */
trait ClientIdTopic {
}

case class ClientIdAndTopic(clientId: String, topic: String) extends ClientIdTopic {
  override def toString = "%s-%s".format(clientId, topic)
}

case class ClientIdAllTopics(clientId: String) extends ClientIdTopic {
  override def toString = "%s-%s".format(clientId, "AllTopics")
}


