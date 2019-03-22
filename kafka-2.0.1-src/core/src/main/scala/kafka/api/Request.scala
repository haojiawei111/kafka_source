
package kafka.api

object Request {
  val OrdinaryConsumerId: Int = -1
  val DebuggingConsumerId: Int = -2
  val FutureLocalReplicaId: Int = -3

  // Broker ids are non-negative int.
  def isValidBrokerId(brokerId: Int): Boolean = brokerId >= 0
}
