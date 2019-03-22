

package kafka.common

/**
  * Indicates the brokerId stored in logDirs is not consistent across logDirs.
  */
class InconsistentBrokerIdException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)
  def this() = this(null, null)
}
