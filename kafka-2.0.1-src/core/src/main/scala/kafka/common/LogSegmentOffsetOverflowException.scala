
package kafka.common

import kafka.log.LogSegment

/**
 * Indicates that the log segment contains one or more messages that overflow the offset (and / or time) index. This is
 * not a typical scenario, and could only happen when brokers have log segments that were created before the patch for
 * KAFKA-5413. With KAFKA-6264, we have the ability to split such log segments into multiple log segments such that we
 * do not have any segments with offset overflow.
 */
class LogSegmentOffsetOverflowException(val segment: LogSegment, val offset: Long)
  extends org.apache.kafka.common.KafkaException(s"Detected offset overflow at offset $offset in segment $segment") {
}
