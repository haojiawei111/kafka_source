

package kafka.common

/**
  * Thrown when there is a failure to generate a zookeeper sequenceId to use as brokerId
  */
class GenerateBrokerIdException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)
  def this() = this(null, null)
}
