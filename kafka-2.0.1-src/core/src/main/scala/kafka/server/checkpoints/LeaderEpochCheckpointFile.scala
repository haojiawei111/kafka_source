package kafka.server.checkpoints

import java.io._
import java.util.regex.Pattern

import kafka.server.LogDirFailureChannel
import kafka.server.epoch.EpochEntry

import scala.collection._

trait LeaderEpochCheckpoint {
  def write(epochs: Seq[EpochEntry])
  def read(): Seq[EpochEntry]
}

object LeaderEpochFile {
  private val LeaderEpochCheckpointFilename = "leader-epoch-checkpoint"
  def newFile(dir: File): File = new File(dir, LeaderEpochCheckpointFilename)
}

object LeaderEpochCheckpointFile {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private val CurrentVersion = 0

  // 写入文件的格式
  object Formatter extends CheckpointFileFormatter[EpochEntry] {

    override def toLine(entry: EpochEntry): String = s"${entry.epoch} ${entry.startOffset}"

    override def fromLine(line: String): Option[EpochEntry] = {
      WhiteSpacesPattern.split(line) match {
        case Array(epoch, offset) =>
          Some(EpochEntry(epoch.toInt, offset.toLong))
        case _ => None
      }
    }
  }

}

/**
  * This class persists a map of (LeaderEpoch => Offsets) to a file (for a certain replica)
  * 此类将（LeaderEpoch => Offsets）的映射保存到文件（对于某个副本）
  */
class LeaderEpochCheckpointFile(val file: File, logDirFailureChannel: LogDirFailureChannel = null) extends LeaderEpochCheckpoint {
  import LeaderEpochCheckpointFile._

  val checkpoint = new CheckpointFile[EpochEntry](file, CurrentVersion, Formatter, logDirFailureChannel, file.getParentFile.getParent)

  def write(epochs: Seq[EpochEntry]): Unit = checkpoint.write(epochs)

  def read(): Seq[EpochEntry] = checkpoint.read()
}
