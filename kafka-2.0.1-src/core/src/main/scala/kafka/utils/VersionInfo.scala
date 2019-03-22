
package kafka.utils

import org.apache.kafka.common.utils.AppInfoParser
// 输出kafka版本信息
object VersionInfo {

  def main(args: Array[String]) {
    val version = AppInfoParser.getVersion
    val commitId = AppInfoParser.getCommitId
    System.out.println(s"${version} (Commit:${commitId})")
    System.exit(0)
  }
}
