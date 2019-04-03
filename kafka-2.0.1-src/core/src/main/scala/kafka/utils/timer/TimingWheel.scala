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
package kafka.utils.timer

import kafka.utils.nonthreadsafe

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

/*
 * 分层定时轮
 *
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 * 一个简单的计时轮是计时器任务的循环列表。让我成为时间单位。大小为n的定时轮有n个桶，可以在n * u时间间隔内保存定时器任务。每个存储桶都包含属于相应时间范围的计时器任务。
 * 开始时，第一个桶保存[0，u）的任务，第二个桶保存[u，2u]，...的任务，[u *（n-1），u * n）的第n个桶。每个时间间隔单位u，计时器滴答并移动到下一个桶然后使其中的所有计时器任务到期。
 * 因此，计时器永远不会将任务插入到当前时间的桶中，因为它已经过期。计时器立即运行过期的任务。然后，清空桶可用于下一轮，因此如果当前桶为时间t，则在勾选后它成为[t + u * n，t +（n + 1）* u）的桶。
 * 定时轮插入/删除（启动定时器/停止定时器）的成本为O（1），而基于优先级队列的定时器（如java.util.concurrent.DelayQueue和java.util.Timer）具有O（log n）插入/删除成本。
 *
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 * 简单定时轮的主要缺点是它假定定时器请求在当前时间n * u的时间间隔内。如果定时器请求超出此间隔，则为溢出。分层计时轮处理这种溢出。
 * 它是一个分层组织的计时轮。最低级别具有最佳时间分辨率。随着层次结构的升级，时间分辨率变得更粗糙。如果一个级别的轮子的分辨率为u且大小为n，则下一级别的分辨率应为n * u。
 * 在每个级别，溢出被委托给更高一级的车轮。当更高级别的车轮打勾时，它会将计时器任务重新插入到较低级别。可以按需创建溢流轮。当溢出桶中的存储桶到期时，其中的所有任务将以递归方式重新插入计时器。
 * 然后将任务移动到更精细的颗粒轮或执行。插入（启动定时器）成本为O（m），其中m是轮数，与系统中的请求数相比通常非常小，并且删除（停止计时器）成本仍为O（1 ）。
 *
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,假设你是1，n是3.如果开始时间是c，
 * then the buckets at different levels are: 那么不同级别的桶是：
 *
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 *
 * The bucket expiration is at the time of bucket beginning.桶期满是在桶开始时。
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired. 因此，在time = c + 1时，桶[c，c]，[c，c + 2]和[c，c + 8]到期。
 * Level 1's clock moves to c+1, and [c+3,c+3] is created. 1级时钟移至c + 1，并创建[c + 3，c + 3]。
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively. Level 2和level3的时钟保持在c，因为它们的时钟分别以3和9为单位移动。
 * So, no new buckets are created in level 2 and 3.因此，在第2级和第3级中不会创建新的存储桶。
 *
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 *
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 *
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+18]
 *
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 *
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+8,c+11]
 *
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 *
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 */

// wheelSize: Int 当前时间轮的大小也就是总的时间格数量
// startMs：Long 当期时间轮的创建时间
// tickMs：Long 当前时间轮中一个时间格表示的时间跨度
// queue:DelayQueue[TimerTaskList] 整个层级的时间轮公用一个任务队列，其元素类型是TimerTaskList
// taskCounter：AtomicInteger 各层级时间轮中任务的总数

@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {


  /***************************当前时间轮只能处理时间范围在currentTime~currentTime+tickMs*WheelSize之间的定时任务，超过这个范围则需要添加任务到上层时间轮*************************************/
  // interval：Long 当前时间轮的时间跨度即tickMs * wheelSize,
  private[this] val interval = tickMs * wheelSize
  //开始时间取整
  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs 四舍五入到多个tickMs
  /******************************************************************************************************************************************************************************/


  //buckets : Array.tabulate[TimerTaskList] 类型，数组大下为wheelSize，其每一个元素都对应时间轮中一个时间格，用于保存TimerTaskList，
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) } //索引是0的存放着这个时间论的时间大的任务，索引是wheelSize的存放着这个时间论的时间小的任务


  // overflowWheel can potentially be updated and read by two concurrent threads through add().
  // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
  // overflowWheel: TimingWheel 上层时间轮的引用
  @volatile private[this] var overflowWheel: TimingWheel = null


  // 添加一层时间轮 主要用于创建上层时间轮
  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        // tickMs = interval 上层时间轮的时间格跨度 = 下一层时间轮的跨度
        // wheelSize = wheelSize 大小不变
        // startMs = currentTime 初始化当前时间轮的创建时间
        // taskCounter = taskCounter 时间轮中任务的总数
        // queue
        overflowWheel = new TimingWheel(tickMs = interval, wheelSize = wheelSize, startMs = currentTime, taskCounter = taskCounter, queue)
      }
    }
  }

  // 向时间轮中添加定时任务，同时也会检测添加的任务是否已经到期
  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    //拿到任务到期时间
    val expiration = timerTaskEntry.expirationMs

    if (timerTaskEntry.cancelled) {
      // Cancelled
      false
    } else if (expiration < currentTime + tickMs) {
      // Already expired 已经过期了
      false
    } else if (expiration < currentTime + interval) {
      // Put in its own bucket 放入自己的桶中
      val virtualId : Long = expiration / tickMs //计算放到那个时间格子
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      bucket.add(timerTaskEntry)

      // Set the bucket expiration time 设置存储桶到期时间
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        queue.offer(bucket)
      }
      true
    } else {
      // Out of the interval. Put it into the parent timer
      if (overflowWheel == null) addOverflowWheel()
      //梵高下一个桶中
      overflowWheel.add(timerTaskEntry)
    }
  }

  // 尝试推进时钟
  def advanceClock(timeMs: Long): Unit = {
    if (timeMs >= currentTime + tickMs) {
      currentTime = timeMs - (timeMs % tickMs)

      // Try to advance the clock of the overflow wheel if present
      // 如果存在，尝试提前溢流轮的时钟
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}
