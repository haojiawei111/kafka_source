
package kafka.utils

import java.util.concurrent._
import atomic._
import org.apache.kafka.common.utils.KafkaThread

/**
 * 用于运行作业的调度程序
 * 
 * This interface controls a job scheduler that allows scheduling either repeating background jobs 
 * that execute periodically or delayed one-time actions that are scheduled in the future.
  * 他的界面控制一个作业调度程序，它允许安排定期执行的重复后台作业*或延迟将来安排的一次性操作。
 */
trait Scheduler {
  
  /**
   * 初始化此调度程序，以便它可以接受任务的调度
   */
  def startup()
  
  /**
   * Shutdown this scheduler. When this method is complete no more executions of background tasks will occur. 
   * This includes tasks scheduled with a delayed execution.
    * 关闭此调度程序。完成此方法后，将不再执行后台任务。 *这包括延迟执行的预定任务。
   */
  def shutdown()
  
  /**
   * Check if the scheduler has been started
    * 检查调度程序是否已启动
   */
  def isStarted: Boolean
  
  /**
   * 安排任务
   * @param name The name of this task
   * @param delay The amount of time to wait before the first execution
   * @param period The period with which to execute the task. If < 0 the task will execute only once.
   * @param unit The unit for the preceding times.
   */
  def schedule(name: String, fun: ()=>Unit, delay: Long = 0, period: Long = -1, unit: TimeUnit = TimeUnit.MILLISECONDS)
}

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 * 
 * It has a pool of kafka-scheduler- threads that do the actual work.
  *
  * 接收需周期性执行的任务和延迟作务的添加
  * 使用一组thread
 * 
 * @param threads The number of threads in the thread pool
 * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
 * @param daemon If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
 */
@threadsafe
class KafkaScheduler(val threads: Int, val threadNamePrefix: String = "kafka-scheduler-", daemon: Boolean = true) extends Scheduler with Logging {
  // 调度线程池
  private var executor: ScheduledThreadPoolExecutor = null
  private val schedulerThreadId = new AtomicInteger(0)

  override def startup() {
    debug("Initializing task scheduler.")
    this synchronized {
      if(isStarted)
        throw new IllegalStateException("This scheduler has already been started!")
      //创建后台定时调度线程池，大小为threads
      executor = new ScheduledThreadPoolExecutor(threads)
      executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)
      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
      //这里没有创建线程，只定义了创建线程的工厂，定时任务全部是创建的KafkaThread类，KafkaThread类是个runnable包装类
      executor.setThreadFactory(new ThreadFactory() {
                                  def newThread(runnable: Runnable): Thread = 
                                    new KafkaThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon)
                                })
    }
  }
  
  override def shutdown() {
    debug("Shutting down task scheduler.")
    //如果另一个线程同时关闭调度程序，我们使用局部变量来避免NullPointerException。
    val cachedExecutor = this.executor
    if (cachedExecutor != null) {
      this synchronized {
        cachedExecutor.shutdown()
        this.executor = null
      }
      cachedExecutor.awaitTermination(1, TimeUnit.DAYS)
    }
  }

  // 这里调度一次并且立即执行
  def scheduleOnce(name: String, fun: () => Unit): Unit = {
    schedule(name, fun, delay = 0L, period = -1L, unit = TimeUnit.MILLISECONDS)
  }
  // 提交调度任务
  def schedule(name: String, fun: () => Unit, delay: Long, period: Long, unit: TimeUnit) {
    debug("Scheduling task %s with initial delay %d ms and period %d ms.".format(name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit)))
    this synchronized {
      ensureRunning()
      val runnable = CoreUtils.runnable {
        try {
          trace("Beginning execution of scheduled task '%s'.".format(name))
          fun()
        } catch {
          case t: Throwable => error("Uncaught exception in scheduled task '" + name +"'", t)
        } finally {
          trace("Completed execution of scheduled task '%s'.".format(name))
        }
      }
      if(period >= 0)//period 两次开始执行最小间隔时间
        executor.scheduleAtFixedRate(runnable, delay, period, unit)
      else
        executor.schedule(runnable, delay, unit)
    }
  }

  // 调整线程池大小
  def resizeThreadPool(newSize: Int): Unit = {
    executor.setCorePoolSize(newSize)
  }
  
  def isStarted: Boolean = {
    this synchronized {
      executor != null
    }
  }

    // 确保在运行
  private def ensureRunning(): Unit = {
    if (!isStarted)
      throw new IllegalStateException("Kafka scheduler is not running.")
  }
}
