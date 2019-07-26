/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io._
import java.nio.file.Files
import java.util
import java.util.concurrent._

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.{BrokerState, RecoveringFromUncleanShutdown, _}
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.errors.{KafkaStorageException, LogDirNotFoundException}

import scala.collection.JavaConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer

object LogManager {
  //恢复点检查点文件
  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  //记录开始偏移检查点文件
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"
  val ProducerIdExpirationCheckIntervalMs = 10 * 60 * 1000

  def apply(config: KafkaConfig,initialOfflineDirs: Seq[String],zkClient: KafkaZkClient,brokerState: BrokerState,
            kafkaScheduler: KafkaScheduler,time: Time,brokerTopicStats: BrokerTopicStats,logDirFailureChannel: LogDirFailureChannel): LogManager = {

    val defaultProps : util.Map[String, Object] = KafkaServer.copyKafkaConfigToLog(config)

    val defaultLogConfig = LogConfig(defaultProps)

    // read the log configurations from zookeeper   从zookeeper中读取日志配置
    //   zkClient.getAllTopicsInCluster 从zookeeper中拿到左右的topic
    val (topicConfigs, failed) = zkClient.getLogConfigs(zkClient.getAllTopicsInCluster, defaultProps)

    if (!failed.isEmpty) throw failed.head._2

    // 清理线程的相关配置
    val cleanerConfig = LogCleaner.cleanerConfig(config)

    new LogManager(
      // 配置文件中的log.dirs
      logDirs = config.logDirs.map(new File(_).getAbsoluteFile),
      // 初始化失败的目录，这些目录包含在log.dirs中
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
      // topic的配置信息
      topicConfigs = topicConfigs,
      // 默认的日志配置
      initialDefaultConfig = defaultLogConfig,
      // 清理线程的配置
      cleanerConfig = cleanerConfig,
      // 配置文件中 num.recovery.threads.per.data.dir 每个数据目录的线程数，用于启动时的日志恢复和关闭时的刷新
      recoveryThreadsPerDataDir = config.numRecoveryThreadsPerDataDir,
      // log.flush.scheduler.interval.ms 日志刷新器检查是否需要将任何日志刷新到磁盘的频率（以毫秒为单位）
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      // log.flush.offset.checkpoint.interval.ms 我们更新上次刷新的持久记录的频率，该记录充当日志恢复点
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      // log.flush.start.offset.checkpoint.interval.ms 我们更新日志开始偏移的持久记录的频率
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      // log.retention.check.interval.ms 日志清理程序检查是否有资格删除日志的频率（以毫秒为单位）
      retentionCheckMs = config.logCleanupIntervalMs,
      // transactional.id.expiration.ms 事务协调器在主动过期生产者的事务ID之前将等待的最长时间（以毫秒为单位），而不从其接收任何事务状态更新。
      maxPidExpirationMs = config.transactionIdExpirationMs,
      // kafka 调度线程池
      scheduler = kafkaScheduler,
      // broker状态
      brokerState = brokerState,
      // broker topic状态
      brokerTopicStats = brokerTopicStats,
      // 初始化失败的目录的Channel，这些目录包含在log.dirs中
      logDirFailureChannel = logDirFailureChannel,
      time = time
    )

  }
}

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
  * kafka日志管理子系统的入口点。日志管理器负责日志创建，检索和清理。 *所有读写操作都委托给各个日志实例。
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
  * 日志管理器将日志保存在一个或多个目录中。使用最少的日志在数据目录*中创建新日志。根据*大小或I / O速率，不会尝试在事实或余额之后移动分区。
 *
 * A background thread handles log retention by periodically truncating excess log segments.
  * 后台线程通过定期截断多余的日志段来处理日志保留。
 */
@threadsafe
class LogManager(logDirs: Seq[File],
                 initialOfflineDirs: Seq[File],
                 val topicConfigs: Map[String, LogConfig], // note that this doesn't get updated after creation请注意，创建后不会更新
                 val initialDefaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 recoveryThreadsPerDataDir: Int,
                 val flushCheckMs: Long,
                 val flushRecoveryOffsetCheckpointMs: Long,
                 val flushStartOffsetCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 val maxPidExpirationMs: Int,
                 scheduler: Scheduler,
                 val brokerState: BrokerState,
                 brokerTopicStats: BrokerTopicStats,
                 logDirFailureChannel: LogDirFailureChannel,
                 time: Time) extends Logging with KafkaMetricsGroup {

  import LogManager._

  // 锁文件
  val LockFile = ".lock"
  //初始任务延迟Ms
  val InitialTaskDelayMs = 30 * 1000

  private val logCreationOrDeletionLock = new Object

  // 内存池 当前日志
  private val currentLogs = new Pool[TopicPartition, Log]()

  // Future logs are put in the directory with "-future" suffix. Future log is created when user wants to move replica
  // from one log directory to another log directory on the same broker. The directory of the future log will be renamed
  // to replace the current log of the partition after the future log catches up with the current log
  // 将来的日志放在带有“-future”后缀的目录中。当用户想要将副本
  // 从一个日志目录移动到同一代理上的另一个日志目录时，将创建将来的日志。未来日志的目录将被重命名
  // 以在将来的日志赶上当前日志后替换分区的当前日志
  // 内存池 未来的日志
  private val futureLogs = new Pool[TopicPartition, Log]()


  // Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion.
  // 队列中的每个元素都包含要删除的日志对象以及计划删除的时间。  要删除的日志
  private val logsToBeDeleted = new LinkedBlockingQueue[(Log, Long)]()

  // 可用日志目录
  private val _liveLogDirs: ConcurrentLinkedQueue[File] = createAndValidateLogDirs(logDirs, initialOfflineDirs)
  // 默认配置
  @volatile private var _currentDefaultConfig = initialDefaultConfig

  // 每个数据目录的线程数，用于启动时的日志恢复和关闭时的刷新
  @volatile private var numRecoveryThreadsPerDataDir = recoveryThreadsPerDataDir

  //重新配置默认日志
  def reconfigureDefaultLogConfig(logConfig: LogConfig): Unit = {
    this._currentDefaultConfig = logConfig
  }

  //返回当前默认配置
  def currentDefaultConfig: LogConfig = _currentDefaultConfig

  // 返回可用日志目录
  def liveLogDirs: Seq[File] = {
    if (_liveLogDirs.size == logDirs.size)
      logDirs
    else
      _liveLogDirs.asScala.toBuffer
  }

  // 锁定可用日志目录 ，添加文件
  private val dirLocks = lockLogDirs(liveLogDirs)

  // map <dir,OffsetCheckpointFile>   recovery-point-offset-checkpoint  //恢复点检查点文件
  @volatile private var recoveryPointCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel))).toMap

  // map<dir,OffsetCheckpointFile>    log-start-offset-checkpoint  //记录开始偏移检查点文件
  @volatile private var logStartOffsetCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel))).toMap

  // map<TopicPartition,String>
  private val preferredLogDirs = new ConcurrentHashMap[TopicPartition, String]()

  //
  private def offlineLogDirs: Iterable[File] = {
    val logDirsSet = mutable.Set[File](logDirs: _*)
    // 从logDirsSet中逐个减去_liveLogDirs，剩下就是目前不可用的离线目录
    _liveLogDirs.asScala.foreach(logDirsSet -=)

    logDirsSet
  }

  loadLogs()

  // public, so we can access this from kafka.admin.DeleteTopicTest public，所以我们可以从kafka.admin.DeleteTopicTest访问它
  // 这里创建清理线程，默认是开始的
  val cleaner: LogCleaner =
    if(cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, liveLogDirs, currentLogs, logDirFailureChannel, time = time)
    else
      null

  val offlineLogDirectoryCount = newGauge(
    "OfflineLogDirectoryCount",
    new Gauge[Int] {
      def value = offlineLogDirs.size
    }
  )

  for (dir <- logDirs) {
    newGauge(
      "LogDirectoryOffline",
      new Gauge[Int] {
        def value = if (_liveLogDirs.contains(dir)) 0 else 1
      },
      Map("logDirectory" -> dir.getAbsolutePath)
    )
  }

  /**
   * Create and check validity of the given directories that are not in the given offline directories, specifically:
    * 创建并检查给定目录中不在给定脱机目录中的有效性，具体如下：
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list 确保目录列表中没有重复项
   * <li> Create each directory if it doesn't exist 创建每个目录（如果不存在）
   * <li> Check that each path is a readable directory 检查每个路径是否为可读目录
   * </ol>
   */
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    // 可用的日志目录
    val liveLogDirs = new ConcurrentLinkedQueue[File]()

    val canonicalPaths = mutable.HashSet.empty[String]

    for (dir <- dirs) {
      try {

        // 如果initialOfflineDirs里面包含dir，抛异常
        if (initialOfflineDirs.contains(dir))
          throw new IOException(s"Failed to load ${dir.getAbsolutePath} during broker startup")

        // 如果没有目录就创建
        if (!dir.exists) {
          info(s"Log directory ${dir.getAbsolutePath} not found, creating it.")
          val created = dir.mkdirs()
          if (!created)
            throw new IOException(s"Failed to create data directory ${dir.getAbsolutePath}")
        }
        // 是否是目录  是否可读
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(s"${dir.getAbsolutePath} is not a readable log directory.")

        // getCanonicalPath() throws IOException if a file system query fails or if the path is invalid (e.g. contains
        // the Nul character). Since there's no easy way to distinguish between the two cases, we treat them the same
        // and mark the log directory as offline.
        // 如果文件系统查询失败或路径无效（例如包含
        // Nul字符），则getCanonicalPath（）将抛出IOException。由于没有简单的方法来区分这两种情况，我们将它们视为相同
        // 并将日志目录标记为离线。
        if (!canonicalPaths.add(dir.getCanonicalPath))
          throw new KafkaException(s"Duplicate log directory found: ${dirs.mkString(", ")}")


        liveLogDirs.add(dir)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Failed to create or validate data directory ${dir.getAbsolutePath}", e)
      }
    }

    // 如果没有可用目录，退出
    if (liveLogDirs.isEmpty) {
      fatal(s"Shutdown broker because none of the specified log dirs from ${dirs.mkString(", ")} can be created or validated")
      Exit.halt(1)
    }

    liveLogDirs
  }

  // 调整恢复线程池的大小
  def resizeRecoveryThreadPool(newSize: Int): Unit = {
    info(s"Resizing recovery thread pool size for each data dir from $numRecoveryThreadsPerDataDir to $newSize")
    numRecoveryThreadsPerDataDir = newSize
  }


  // dir应该是一绝对路径
  def handleLogDirFailure(dir: String) {
    info(s"Stopping serving logs in dir $dir")
    logCreationOrDeletionLock synchronized {
      _liveLogDirs.remove(new File(dir))
      if (_liveLogDirs.isEmpty) {
        fatal(s"Shutdown broker because all log dirs in ${logDirs.mkString(", ")} have failed")
        Exit.halt(1)
      }

      recoveryPointCheckpoints = recoveryPointCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      logStartOffsetCheckpoints = logStartOffsetCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      if (cleaner != null)
        cleaner.handleLogDirFailure(dir)

      val offlineCurrentTopicPartitions = currentLogs.collect {
        case (tp, log) if log.dir.getParent == dir => tp
      }
      offlineCurrentTopicPartitions.foreach { topicPartition => {
        val removedLog = currentLogs.remove(topicPartition)
        if (removedLog != null) {
          removedLog.closeHandlers()
          removedLog.removeLogMetrics()
        }
      }}

      val offlineFutureTopicPartitions = futureLogs.collect {
        case (tp, log) if log.dir.getParent == dir => tp
      }
      offlineFutureTopicPartitions.foreach { topicPartition => {
        val removedLog = futureLogs.remove(topicPartition)
        if (removedLog != null) {
          removedLog.closeHandlers()
          removedLog.removeLogMetrics()
        }
      }}

      info(s"Logs for partitions ${offlineCurrentTopicPartitions.mkString(",")} are offline and " +
           s"logs for future partitions ${offlineFutureTopicPartitions.mkString(",")} are offline due to failure on log directory $dir")
      dirLocks.filter(_.file.getParent == dir).foreach(dir => CoreUtils.swallow(dir.destroy(), this))
    }
  }

  /**
   * 锁定所有给定的目录
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.flatMap { dir =>
      try {
        // 获取文件锁
        val lock = new FileLock(new File(dir, LockFile))
        if (!lock.tryLock())
          throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParent +
            ". A Kafka instance in another process or thread is using this directory.")
        Some(lock)

      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while locking directory $dir", e)
          None
      }
    }
  }

  private def addLogToBeDeleted(log: Log): Unit = {
    this.logsToBeDeleted.add((log, time.milliseconds()))
  }

  // Only for testing
  private[log] def hasLogsToBeDeleted: Boolean = !logsToBeDeleted.isEmpty



  // 加载日志 log恢复主要逻辑
  private def loadLog(logDir: File, recoveryPoints: Map[TopicPartition, Long], logStartOffsets: Map[TopicPartition, Long]): Unit = {
    debug("Loading log '" + logDir.getName + "'")
    // 解析文件名
    val topicPartition = Log.parseTopicPartitionName(logDir)
    // 返回topicPartition.topic配置
    val config = topicConfigs.getOrElse(topicPartition.topic, currentDefaultConfig)
    // 日志恢复点 这个是从recovery-point-offset-checkpoint文件里面读出来的
    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)

    // 记录日志开始偏移检查点文件 这个是从log-start-offset-checkpoint文件里面读出来的
    val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)

    val log = Log(
      dir = logDir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = logRecoveryPoint,
      maxProducerIdExpirationMs = maxPidExpirationMs,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      scheduler = scheduler,
      time = time,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel)


    // Log.DeleteDirSuffix  = "-delete"
    if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {

      addLogToBeDeleted(log)

    } else {

      val previous = {
        if (log.isFuture)
          // 如果logDir结尾是-future
          this.futureLogs.put(topicPartition, log)
        else
          this.currentLogs.put(topicPartition, log)
      }

      if (previous != null) {
        if (log.isFuture)
          throw new IllegalStateException("Duplicate log directories found找到重复的日志目录: %s, %s!".format(log.dir.getAbsolutePath, previous.dir.getAbsolutePath))
        else
          throw new IllegalStateException(s"Duplicate log directories for $topicPartition are found in both ${log.dir.getAbsolutePath} " +
            s"and ${previous.dir.getAbsolutePath}. It is likely because log directory failure happened while broker was " +
            s"replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two directories " +
            s"for this partition. It is recommended to delete the partition in the log directory that is known to have failed recently.")
      }
    }
  }

  /**
   * Recover and load all logs in the given data directories
    * 恢复并加载给定数据目录中的所有日志
   */
  private def loadLogs(): Unit = {
    info("Loading logs.")
    val startMs = time.milliseconds
    // 线程池集合 一堆线程池
    val threadPools = ArrayBuffer.empty[ExecutorService]
    // 离线日志
    val offlineDirs = mutable.Set.empty[(String, IOException)]

    // map<File,Seq[Future[_]]>     每个文件对应一个Future集合
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // 遍历每个目录
    for (dir <- liveLogDirs) {
      try {
        // 固定大小的线程池，newFixedThreadPool：
        val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
        threadPools.append(pool)

        // 清理关闭文件  如果有此文件表明kakfa正常关闭，log不需要recovery
        val cleanShutdownFile = new File(dir, Log.CleanShutdownFile) //Log.CleanShutdownFile  .kafka_cleanshutdown

        if (cleanShutdownFile.exists) {
          debug(s"Found clean shutdown file. Skipping recovery for all logs in data directory找到干净的关机文件。跳过数据目录中所有日志的恢复: ${dir.getAbsolutePath}")
        } else {
          // log recovery itself is being performed by `Log` class during initialization 日志恢复本身在初始化期间由`Log`类执行  这里改变broker状态
          brokerState.newState(RecoveringFromUncleanShutdown)
        }

        // 恢复点   map<TopicPartition, Long>
        var recoveryPoints = Map[TopicPartition, Long]()

        try {
          recoveryPoints = this.recoveryPointCheckpoints(dir).read
        } catch {
          case e: Exception =>
            warn("Error occurred while reading recovery-point-offset-checkpoint file of directory " + dir, e)
            warn("Resetting the recovery checkpoint to 0")
        }
        // log开始偏移
        var logStartOffsets = Map[TopicPartition, Long]()

        try {
          logStartOffsets = this.logStartOffsetCheckpoints(dir).read
        } catch {
          case e: Exception =>
            warn("Error occurred while reading log-start-offset-checkpoint file of directory " + dir, e)
        }


        val jobsForDir = for {
          // 子文件夹  这里要便利每个topic里面的文件    logDir是配置文件里添加的文件路径下面的目录
          dirContent <- Option(dir.listFiles).toList
          logDir <- dirContent if logDir.isDirectory
        } yield {
          CoreUtils.runnable {
            try {
              loadLog(logDir, recoveryPoints, logStartOffsets)
            } catch {
              case e: IOException =>
                offlineDirs.add((dir.getAbsolutePath, e))
                error("Error while loading log dir " + dir.getAbsolutePath, e)
            }
          }
        }
        // 如果cleanShutdownFile不存在，则上一次关闭不是正常关闭
        jobs(cleanShutdownFile) = jobsForDir.map(pool.submit)

      } catch {
        case e: IOException =>
          // 恢复过程中如果有异常加入离线目录集合
          offlineDirs.add((dir.getAbsolutePath, e))
          error("Error while loading log dir " + dir.getAbsolutePath, e)
      }
    }

    //
    try {
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        // 拿到dirJobs线程返回值 ，这里get会阻塞
        dirJobs.foreach(_.get)
        try {
          // 删除 清理关机文件
          cleanShutdownFile.delete()
        } catch {
          case e: IOException =>
            offlineDirs.add((cleanShutdownFile.getParent, e))
            error(s"Error while deleting the clean shutdown file $cleanShutdownFile", e)
        }
      }

      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while deleting the clean shutdown file in dir在dir中删除干净关闭文件时出错 $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        error("There was an error in one of the threads during logs loading: " + e.getCause)
        throw e.getCause
    } finally {
      // 关闭线程池
      threadPools.foreach(_.shutdown())
    }

    info(s"Logs loading complete in ${time.milliseconds - startMs} ms.")
  }


  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup() {
    /* Schedule the cleanup task to delete old logs */
    if (scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs _,
                         delay = InitialTaskDelayMs,
                         period = retentionCheckMs,
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs _,
                         delay = InitialTaskDelayMs,
                         period = flushCheckMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointLogRecoveryOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushRecoveryOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         checkpointLogStartOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushStartOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-delete-logs", // will be rescheduled after each delete logs with a dynamic period
                         deleteLogs _,
                         delay = InitialTaskDelayMs,
                         unit = TimeUnit.MILLISECONDS)
    }
    if (cleanerConfig.enableCleaner)
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown() {
    info("Shutting down.")

    removeMetric("OfflineLogDirectoryCount")
    for (dir <- logDirs) {
      removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath))
    }

    val threadPools = ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown(), this)
    }

    // close logs in each dir
    for (dir <- liveLogDirs) {
      debug("Flushing and closing logs at " + dir)

      val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
      threadPools.append(pool)

      val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values

      val jobsForDir = logsInDir map { log =>
        CoreUtils.runnable {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        // update the last flush point
        debug("Updating recovery points at " + dir)
        checkpointLogRecoveryOffsetsInDir(dir)

        debug("Updating log start offsets at " + dir)
        checkpointLogStartOffsetsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug("Writing clean shutdown marker at " + dir)
        CoreUtils.swallow(Files.createFile(new File(dir, Log.CleanShutdownFile).toPath), this)
      }
    } catch {
      case e: ExecutionException =>
        error("There was an error in one of the threads during LogManager shutdown: " + e.getCause)
        throw e.getCause
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }

  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   * @param isFuture True iff the truncation should be performed on the future log of the specified partitions
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long], isFuture: Boolean) {
    var truncated = false
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = {
        if (isFuture)
          futureLogs.get(topicPartition)
        else
          currentLogs.get(topicPartition)
      }
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner = cleaner != null && truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && !isFuture)
          cleaner.abortAndPauseCleaning(topicPartition)
        try {
          if (log.truncateTo(truncateOffset))
            truncated = true
          if (needToStopCleaner && !isFuture)
            cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
        } finally {
          if (needToStopCleaner && !isFuture)
            cleaner.resumeCleaning(topicPartition)
        }
      }
    }

    if (truncated)
      checkpointLogRecoveryOffsets()
  }

  /**
   * Delete all data in a partition and start the log at the new offset
   *
   * @param topicPartition The partition whose log needs to be truncated
   * @param newOffset The new offset to start the log with
   * @param isFuture True iff the truncation should be performed on the future log of the specified partition
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long, isFuture: Boolean) {
    val log = {
      if (isFuture)
        futureLogs.get(topicPartition)
      else
        currentLogs.get(topicPartition)
    }
    // If the log does not exist, skip it
    if (log != null) {
        //Abort and pause the cleaning of the log, and resume after truncation is done.
      if (cleaner != null && !isFuture)
        cleaner.abortAndPauseCleaning(topicPartition)
      log.truncateFullyAndStartAt(newOffset)
      if (cleaner != null && !isFuture) {
        cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
        cleaner.resumeCleaning(topicPartition)
      }
      checkpointLogRecoveryOffsetsInDir(log.dir.getParentFile)
    }
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory
   * to avoid recovering the whole log on startup.
   */
  def checkpointLogRecoveryOffsets() {
    liveLogDirs.foreach(checkpointLogRecoveryOffsetsInDir)
  }

  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   */
  def checkpointLogStartOffsets() {
    liveLogDirs.foreach(checkpointLogStartOffsetsInDir)
  }

  /**
   * Make a checkpoint for all logs in provided directory.
   */
  private def checkpointLogRecoveryOffsetsInDir(dir: File): Unit = {
    for {
      partitionToLog <- logsByDir.get(dir.getAbsolutePath)
      checkpoint <- recoveryPointCheckpoints.get(dir)
    } {
      try {
        checkpoint.write(partitionToLog.mapValues(_.recoveryPoint))
        allLogs.foreach(_.deleteSnapshotsAfterRecoveryPointCheckpoint())
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while writing to recovery point " +
            s"file in directory $dir", e)
      }
    }
  }

  /**
   * Checkpoint log start offset for all logs in provided directory.
   */
  private def checkpointLogStartOffsetsInDir(dir: File): Unit = {
    for {
      partitionToLog <- logsByDir.get(dir.getAbsolutePath)
      checkpoint <- logStartOffsetCheckpoints.get(dir)
    } {
      try {
        val logStartOffsets = partitionToLog.filter { case (_, log) =>
          log.logStartOffset > log.logSegments.head.baseOffset
        }.mapValues(_.logStartOffset)
        checkpoint.write(logStartOffsets)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while writing to logStartOffset file in directory $dir", e)
      }
    }
  }

  // The logDir should be an absolute path
  def maybeUpdatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // Do not cache the preferred log directory if either the current log or the future log for this partition exists in the specified logDir
    if (!getLog(topicPartition).exists(_.dir.getParent == logDir) &&
        !getLog(topicPartition, isFuture = true).exists(_.dir.getParent == logDir))
      preferredLogDirs.put(topicPartition, logDir)
  }

  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null)
      cleaner.abortAndPauseCleaning(topicPartition)
  }


  /**
   * Get the log if it exists, otherwise return None
   *
   * @param topicPartition the partition of the log
   * @param isFuture True iff the future log of the specified partition should be returned
   */
  def getLog(topicPartition: TopicPartition, isFuture: Boolean = false): Option[Log] = {
    if (isFuture)
      Option(futureLogs.get(topicPartition))
    else
      Option(currentLogs.get(topicPartition))
  }

  /**
   * If the log already exists, just return a copy of the existing log
   * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
   * Otherwise throw KafkaStorageException
   *
   * @param topicPartition The partition whose log needs to be returned or created
   * @param config The configuration of the log that should be applied for log creation
   * @param isNew Whether the replica should have existed on the broker or not
   * @param isFuture True iff the future log of the specified partition should be returned or created
   * @throws KafkaStorageException if isNew=false, log is not found in the cache and there is offline log directory on the broker
   */
  def getOrCreateLog(topicPartition: TopicPartition, config: LogConfig, isNew: Boolean = false, isFuture: Boolean = false): Log = {
    logCreationOrDeletionLock synchronized {
      getLog(topicPartition, isFuture).getOrElse {
        // create the log if it has not already been created in another thread
        if (!isNew && offlineLogDirs.nonEmpty)
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")

        val logDir = {
          val preferredLogDir = preferredLogDirs.get(topicPartition)

          if (isFuture) {
            if (preferredLogDir == null)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition without having a preferred log directory")
            else if (getLog(topicPartition).get.dir.getParent == preferredLogDir)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition in the current log directory of this partition")
          }

          if (preferredLogDir != null)
            preferredLogDir
          else
            nextLogDir().getAbsolutePath
        }
        if (!isLogDirOnline(logDir))
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directory $logDir is offline")

        try {
          val dir = {
            if (isFuture)
              new File(logDir, Log.logFutureDirName(topicPartition))
            else
              new File(logDir, Log.logDirName(topicPartition))
          }
          Files.createDirectories(dir.toPath)

          val log = Log(
            dir = dir,
            config = config,
            logStartOffset = 0L,
            recoveryPoint = 0L,
            maxProducerIdExpirationMs = maxPidExpirationMs,
            producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
            scheduler = scheduler,
            time = time,
            brokerTopicStats = brokerTopicStats,
            logDirFailureChannel = logDirFailureChannel)

          if (isFuture)
            futureLogs.put(topicPartition, log)
          else
            currentLogs.put(topicPartition, log)

          info(s"Created log for partition $topicPartition in $logDir with properties " +
            s"{${config.originals.asScala.mkString(", ")}}.")
          // Remove the preferred log dir since it has already been satisfied
          preferredLogDirs.remove(topicPartition)

          log
        } catch {
          case e: IOException =>
            val msg = s"Error while creating log for $topicPartition in dir $logDir"
            logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
            throw new KafkaStorageException(msg, e)
        }
      }
    }
  }

  /**
   *  Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
   *  has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
   *  considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
   *  after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
   *  `deleteLogs` will be executed after `currentDefaultConfig.fileDeleteDelayMs`.
   */
  private def deleteLogs(): Unit = {
    var nextDelayMs = 0L
    try {
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          scheduleTimeMs + currentDefaultConfig.fileDeleteDelayMs - time.milliseconds()
        } else
          currentDefaultConfig.fileDeleteDelayMs
      }

      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              error(s"Exception while deleting $removedLog in dir ${removedLog.dir.getParent}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Exception in kafka-delete-logs thread.", e)
    } finally {
      try {
        scheduler.schedule("kafka-delete-logs",
          deleteLogs _,
          delay = nextDelayMs,
          unit = TimeUnit.MILLISECONDS)
      } catch {
        case e: Throwable =>
          if (scheduler.isStarted) {
            // No errors should occur unless scheduler has been shutdown
            error(s"Failed to schedule next delete in kafka-delete-logs thread", e)
          }
      }
    }
  }

  /**
    * Mark the partition directory in the source log directory for deletion and
    * rename the future log of this partition in the destination log directory to be the current log
    *
    * @param topicPartition TopicPartition that needs to be swapped
    */
  def replaceCurrentWithFutureLog(topicPartition: TopicPartition): Unit = {
    logCreationOrDeletionLock synchronized {
      val sourceLog = currentLogs.get(topicPartition)
      val destLog = futureLogs.get(topicPartition)

      if (sourceLog == null)
        throw new KafkaStorageException(s"The current replica for $topicPartition is offline")
      if (destLog == null)
        throw new KafkaStorageException(s"The future replica for $topicPartition is offline")

      destLog.renameDir(Log.logDirName(topicPartition))
      // Now that future replica has been successfully renamed to be the current replica
      // Update the cached map and log cleaner as appropriate.
      futureLogs.remove(topicPartition)
      currentLogs.put(topicPartition, destLog)
      if (cleaner != null) {
        cleaner.alterCheckpointDir(topicPartition, sourceLog.dir.getParentFile, destLog.dir.getParentFile)
        cleaner.resumeCleaning(topicPartition)
      }

      try {
        sourceLog.renameDir(Log.logDeleteDirName(topicPartition))
        // Now that replica in source log directory has been successfully renamed for deletion.
        // Close the log, update checkpoint files, and enqueue this log to be deleted.
        sourceLog.close()
        checkpointLogRecoveryOffsetsInDir(sourceLog.dir.getParentFile)
        checkpointLogStartOffsetsInDir(sourceLog.dir.getParentFile)
        addLogToBeDeleted(sourceLog)
      } catch {
        case e: KafkaStorageException =>
          // If sourceLog's log directory is offline, we need close its handlers here.
          // handleLogDirFailure() will not close handlers of sourceLog because it has been removed from currentLogs map
          sourceLog.closeHandlers()
          sourceLog.removeLogMetrics()
          throw e
      }

      info(s"The current replica is successfully replaced with the future replica for $topicPartition")
    }
  }

  /**
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
    * add it in the queue for deletion.
    *
    * @param topicPartition TopicPartition that needs to be deleted
    * @param isFuture True iff the future log of the specified partition should be deleted
    * @return the removed log
    */
  def asyncDelete(topicPartition: TopicPartition, isFuture: Boolean = false): Log = {
    val removedLog: Log = logCreationOrDeletionLock synchronized {
      if (isFuture)
        futureLogs.remove(topicPartition)
      else
        currentLogs.remove(topicPartition)
    }
    if (removedLog != null) {
      //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
      if (cleaner != null && !isFuture) {
        cleaner.abortCleaning(topicPartition)
        cleaner.updateCheckpoints(removedLog.dir.getParentFile)
      }
      removedLog.renameDir(Log.logDeleteDirName(topicPartition))
      checkpointLogRecoveryOffsetsInDir(removedLog.dir.getParentFile)
      checkpointLogStartOffsetsInDir(removedLog.dir.getParentFile)
      addLogToBeDeleted(removedLog)
      info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")
    } else if (offlineLogDirs.nonEmpty) {
      throw new KafkaStorageException("Failed to delete log for " + topicPartition + " because it may be in one of the offline directories " + offlineLogDirs.mkString(","))
    }
    removedLog
  }

  /**
   * Choose the next directory in which to create a log. Currently this is done
   * by calculating the number of partitions in each directory and then choosing the
   * data directory with the fewest partitions.
   */
  private def nextLogDir(): File = {
    if(_liveLogDirs.size == 1) {
      _liveLogDirs.peek()
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
      val dirCounts = (zeros ++ logCounts).toBuffer

      // choose the directory with the least logs in it
      val leastLoaded = dirCounts.sortBy(_._2).head
      new File(leastLoaded._1)
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   */
  def cleanupLogs() {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds
    for(log <- allLogs; if !log.config.compact) {
      debug("Garbage collecting '" + log.name + "'")
      total += log.deleteOldSegments()
    }
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs: Iterable[Log] = currentLogs.values ++ futureLogs.values

  def logsByTopic(topic: String): Seq[Log] = {
    (currentLogs.toList ++ futureLogs.toList).filter { case (topicPartition, _) =>
      topicPartition.topic() == topic
    }.map { case (_, log) => log }
  }

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir: Map[String, Map[TopicPartition, Log]] = {
    (this.currentLogs.toList ++ this.futureLogs.toList).groupBy {
      case (_, log) => log.dir.getParent
    }.mapValues(_.toMap)
  }

  // logDir should be an absolute path
  def isLogDirOnline(logDir: String): Boolean = {
    // The logDir should be an absolute path
    if (!logDirs.exists(_.getAbsolutePath == logDir))
      throw new LogDirNotFoundException(s"Log dir $logDir is not found in the config.")

    _liveLogDirs.contains(new File(logDir))
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  private def flushDirtyLogs(): Unit = {
    debug("Checking for dirty logs to flush...")

    for ((topicPartition, log) <- currentLogs.toList ++ futureLogs.toList) {
      try {
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug("Checking if flush is needed on " + topicPartition.topic + " flush interval  " + log.config.flushMs +
              " last flushed " + log.lastFlushTime + " time since last flush: " + timeSinceLastFlush)
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush
      } catch {
        case e: Throwable =>
          error("Error flushing topic " + topicPartition.topic, e)
      }
    }
  }
}


