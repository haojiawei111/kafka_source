
package kafka.api

object Request {
  val OrdinaryConsumerId: Int = -1
  val DebuggingConsumerId: Int = -2 /* 调试消费者ID */
  val FutureLocalReplicaId: Int = -3 /* 未来的本地副本ID */

  // Broker ids are non-negative int.
  def isValidBrokerId(brokerId: Int): Boolean = brokerId >= 0
}
