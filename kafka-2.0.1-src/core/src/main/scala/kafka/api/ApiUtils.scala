
package kafka.api

import java.nio.ByteBuffer

import org.apache.kafka.common.KafkaException

/**
 * Helper functions specific to parsing or serializing requests and responses
  * Helper函数特定于解析或序列化请求和响应
 */
object ApiUtils {
  
  val ProtocolEncoding = "UTF-8"

    /**
   * Read size prefixed string where the size is stored as a 2 byte short.
   * @param buffer The buffer to read from
   */
  def readShortString(buffer: ByteBuffer): String = {
    val size: Int = buffer.getShort()
    if(size < 0)
      return null
    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    new String(bytes, ProtocolEncoding)
  }
  
  /**
   * Write a size prefixed string where the size is stored as a 2 byte short
   * @param buffer The buffer to write to
   * @param string The string to write
   */
  def writeShortString(buffer: ByteBuffer, string: String) {
    if(string == null) {
      buffer.putShort(-1)
    } else {
      val encodedString = string.getBytes(ProtocolEncoding)
      if(encodedString.length > Short.MaxValue) {
        throw new KafkaException("String exceeds the maximum size of " + Short.MaxValue + ".")
      } else {
        buffer.putShort(encodedString.length.asInstanceOf[Short])
        buffer.put(encodedString)
      }
    }
  }
  
  /**
   * Return size of a size prefixed string where the size is stored as a 2 byte short
   * @param string The string to write
   */
  def shortStringLength(string: String): Int = {
    if(string == null) {
      2
    } else {
      val encodedString = string.getBytes(ProtocolEncoding)
      if(encodedString.length > Short.MaxValue) {
        throw new KafkaException("String exceeds the maximum size of " + Short.MaxValue + ".")
      } else {
        2 + encodedString.length
      }
    }
  }
  
}
