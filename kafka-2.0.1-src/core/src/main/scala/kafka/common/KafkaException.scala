
package kafka.common

/**
 * Usage of this class is discouraged. Use org.apache.kafka.common.KafkaException instead.
 *
 * This class will be removed once ZkUtils and the kafka.security.auth classes are removed.
 * The former is internal, but widely used, so we are leaving it in the codebase for now.
*/
class KafkaException(message: String, t: Throwable) extends RuntimeException(message, t) {
  def this(message: String) = this(message, null)
  def this(t: Throwable) = this("", t)
}
