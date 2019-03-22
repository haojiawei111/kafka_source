package kafka.common


/**
 * Convenience case class since (clientId, brokerInfo) pairs are used to create
 * SyncProducer Request Stats and SimpleConsumer Request and Response Stats.
 */

trait ClientIdBroker {
}

case class ClientIdAndBroker(clientId: String, brokerHost: String, brokerPort: Int) extends ClientIdBroker {
  override def toString = "%s-%s-%d".format(clientId, brokerHost, brokerPort)
}

case class ClientIdAllBrokers(clientId: String) extends ClientIdBroker {
  override def toString = "%s-%s".format(clientId, "AllBrokers")
}
