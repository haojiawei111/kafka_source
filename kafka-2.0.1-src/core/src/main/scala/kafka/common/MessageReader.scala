
package kafka.common

import java.io.InputStream
import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Typical implementations of this interface convert data from an `InputStream` received via `init` into a
  * `ProducerRecord` instance on each invocation of `readMessage`.
  *
  * This is used by the `ConsoleProducer`.
  */
trait MessageReader {

  def init(inputStream: InputStream, props: Properties) {}

  def readMessage(): ProducerRecord[Array[Byte], Array[Byte]]

  def close() {}

}
