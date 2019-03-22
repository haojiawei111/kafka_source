
package kafka.common

class BrokerEndPointNotAvailableException(message: String) extends RuntimeException(message) {
  def this() = this(null)
}
