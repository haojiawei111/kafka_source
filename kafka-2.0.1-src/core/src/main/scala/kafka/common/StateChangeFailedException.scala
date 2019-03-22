
package kafka.common

// 状态改变失败异常
class StateChangeFailedException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this() = this(null, null)
}