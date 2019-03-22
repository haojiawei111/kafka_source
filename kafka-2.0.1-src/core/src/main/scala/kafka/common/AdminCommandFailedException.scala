

package kafka.common

class AdminCommandFailedException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this() = this(null, null)
}
