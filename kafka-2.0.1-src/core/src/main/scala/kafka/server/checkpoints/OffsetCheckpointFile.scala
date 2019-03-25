package kafka.server.checkpoints

import java.io._
import java.util.regex.Pattern

import kafka.server.LogDirFailureChannel
import kafka.server.epoch.EpochEntry
import org.apache.kafka.common.TopicPartition

import scala.collection._

object OffsetCheckpointFile {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private[checkpoints] val CurrentVersion = 0

  // 写入文件的格式
  object Formatter extends CheckpointFileFormatter[(TopicPartition, Long)] {
    override def toLine(entry: (TopicPartition, Long)): String = {
      s"${entry._1.topic} ${entry._1.partition} ${entry._2}"
    }

    override def fromLine(line: String): Option[(TopicPartition, Long)] = {
      WhiteSpacesPattern.split(line) match {
        case Array(topic, partition, offset) =>
          Some(new TopicPartition(topic, partition.toInt), offset.toLong)
        case _ => None
      }
    }
  }
}

trait OffsetCheckpoint {
  def write(epochs: Seq[EpochEntry])
  def read(): Seq[EpochEntry]
}

/**
  * This class persists a map of (Partition => Offsets) to a file (for a certain replica)
  * 此类将（Partition => Offsets）的映射保留到文件（对于某个副本）
  */
class OffsetCheckpointFile(val file: File, logDirFailureChannel: LogDirFailureChannel = null) {

  val checkpoint = new CheckpointFile[(TopicPartition, Long)](file, OffsetCheckpointFile.CurrentVersion,OffsetCheckpointFile.Formatter, logDirFailureChannel, file.getParent)

  // 写offset
  def write(offsets: Map[TopicPartition, Long]): Unit = checkpoint.write(offsets.toSeq)
  // 读 map<TopicPartition, Long>
  def read(): Map[TopicPartition, Long] = checkpoint.read().toMap

}
