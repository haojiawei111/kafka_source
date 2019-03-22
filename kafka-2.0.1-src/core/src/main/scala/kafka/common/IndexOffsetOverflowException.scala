

package kafka.common

/**
 * Indicates that an attempt was made to append a message whose offset could cause the index offset to overflow.
 */
class IndexOffsetOverflowException(message: String, cause: Throwable) extends org.apache.kafka.common.KafkaException(message, cause) {
  def this(message: String) = this(message, null)
}
