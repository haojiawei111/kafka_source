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

import java.io.{File, IOException, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.common.IndexOffsetOverflowException
import kafka.log.IndexSearchType.IndexSearchEntity
import kafka.utils.CoreUtils.inLock
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.utils.{MappedByteBuffers, OperatingSystem, Utils}

import scala.math.ceil

/**
 * The abstract index class which holds entry format agnostic methods.
  * 包含入口格式不可知方法的抽象索引类。
 *
 * @param file The index file
 * @param baseOffset the base offset of the segment that this index is corresponding to. 此索引所对应的segment的基本偏移量。
 * @param maxIndexSize The maximum index size in bytes.
 */
abstract class AbstractIndex[K, V](@volatile var file: File, val baseOffset: Long,val maxIndexSize: Int = -1, val writable: Boolean) extends Logging {

  // Length of the index file索引文件的长度
  @volatile
  private var _length: Long = _

  //条目大小
  protected def entrySize: Int

  protected val lock = new ReentrantLock

  @volatile
  protected var mmap: MappedByteBuffer = {
    // 创建新文件
    val newlyCreated = file.createNewFile()

    // 如果可写 打开文件模式rw 否则是只读
    // RandomAccessFile是用来访问那些保存数据记录的文件的，你就可以用seek( )方法来访问记录，并进行读写了
    val raf = if (writable) new RandomAccessFile(file, "rw") else new RandomAccessFile(file, "r")

    try {
      /* pre-allocate the file if necessary必要时预先分配文件 */
      if(newlyCreated) {  //如果只新建文件
        if(maxIndexSize < entrySize)
          throw new IllegalArgumentException("Invalid max index size无效的最大索引大小: " + maxIndexSize)
        // 设置文件长度
        raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize))
      }

      /* memory-map the file 内存映射文件*/
      //内存映射文件能让你创建和修改那些因为太大而无法放入内存的文件。有了内存映射文件，你就可以认为文件已经全部读进了内存，然后把它当成一个非常大的数组来访问。这种解决办法能大大简化修改文件的代码。
      //fileChannel.map(FileChannel.MapMode mode, long position, long size)将此通道的文件区域直接映射到内存中。
      // 注意，你必须指明，它是从文件的哪个位置开始映射的，映射的范围又有多大；也就是说，它还可以映射一个大文件的某个小片断。
      _length = raf.length()
      val idx = {
        if (writable)
          // 可读写
          raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, _length)
        else
          // 只读
          raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, _length)
      }

      /* set the position in the index for the next entry在下一个条目的索引中设置位置 */
      // 这里把文件指针移动到了要写入数据的位置
      if(newlyCreated)
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is valid and set position to last entry如果这是一个预先存在的索引，则假设它有效并将位置设置为最后一个条目
        idx.position(roundDownToExactMultiple(idx.limit(), entrySize))
      idx
    } finally {
      CoreUtils.swallow(raf.close(), this)
    }
  }

  /**
   * 此索引可以容纳的最大条目数
   */
  @volatile
  private[this] var _maxEntries = mmap.limit() / entrySize

  /** 此索引中已经存在的条目数 */
  @volatile
  protected var _entries = mmap.position() / entrySize

  /**
    * 如果此索引中没有可用的空间了，则为true
   */
  def isFull: Boolean = _entries >= _maxEntries

  def maxEntries: Int = _maxEntries

  def entries: Int = _entries

  def length: Long = _length

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
    * 重置内存映射和底层文件的大小。这用于两种情况：
    * （1）在trimToValidSize（）中，在关闭段或被滚动的新段时调用;
    * （2）在从磁盘加载段或截断到新的日志段变为活动的旧段; 我们希望将索引大小重置为最大索引大小，以避免滚动新段。
   *
   * @param newSize new size of the index file 索引文件的新大小
   * @return a boolean indicating whether the size of the memory map and the underneath file is changed or not.
    *         一个布尔值，指示是否更改了内存映射和底层文件的大小。
   */
  def resize(newSize: Int): Boolean = {
    inLock(lock) {
      // newSize中能存放的最大条数
      val roundedNewSize = roundDownToExactMultiple(newSize, entrySize)

      if (_length == roundedNewSize) {
        false
      } else {
        val raf = new RandomAccessFile(file, "rw")
        try {
          val position = mmap.position()

          /* Windows won't let us modify the file length while the file is mmapped :-( Windows文件被映射时，Windows不会让我们修改文件长度:-(*/
          if (OperatingSystem.IS_WINDOWS)
            safeForceUnmap()
          // 设置行的长度
          raf.setLength(roundedNewSize)
          _length = roundedNewSize
          mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
          _maxEntries = mmap.limit() / entrySize
          mmap.position(position)
          true
        } finally {
          CoreUtils.swallow(raf.close(), this)
        }
      }
    }
  }

  /**
   * Rename the file that backs this offset index
    * 重命名支持此偏移索引的文件
   *
   * @throws IOException if rename fails
   */
  def renameTo(f: File) {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    finally file = f
  }

  /**
   * 将索引中的数据刷新到磁盘
   */
  def flush() {
    inLock(lock) {
      mmap.force()
    }
  }

  /**
   * Delete this index file.
    * 删除此索引文件。
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    inLock(lock) {
      // On JVM, a memory mapping is typically unmapped by garbage collector.
      // However, in some cases it can pause application threads(STW) for a long moment reading metadata from a physical disk.
      // To prevent this, we forcefully cleanup memory mapping within proper execution which never affects API responsiveness.
      // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.
      //在JVM上，内存映射通常由垃圾收集器取消映射。
      // 但是，在某些情况下，它可以暂停应用程序线程（STW）很长时间从物理磁盘读取元数据。
      // 为了防止这种情况，我们强制清理正确执行中的内存映射，这绝不会影响API响应。 //有关详细信息，请参阅https://issues.apache.org/jira/browse/KAFKA-4614。 safeForceUnmap（）
      safeForceUnmap()
    }
    Files.deleteIfExists(file.toPath)
  }

  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
    * 修剪此段以仅适合有效条目，从文件中删除所有尾随未写入的字节。
   */
  def trimToValidSize() {
    inLock(lock) {
      // 此索引实际使用的字节数  每条entrySize乘以此索引中已经存在的条目数
      resize(entrySize * _entries)
    }
  }

  /**
   * 此索引实际使用的字节数
   */
  def sizeInBytes = entrySize * _entries

  /** 关闭索引文件 */
  def close() {
    trimToValidSize()
  }

  // 安全关闭取消映射文件
  def closeHandler(): Unit = {
    inLock(lock) {
      safeForceUnmap()
    }
  }

  /**
    * 对此索引进行基本的健全性检查以检测明显的问题
   *
   * @throws CorruptIndexException if any problems are found
   */
  def sanityCheck(): Unit

  /**
   * 从索引中删除所有条目。
   */
  protected def truncate(): Unit

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
    * 从索引中删除偏移量大于或等于给定偏移量的所有条目。截断大于索引中最大值的偏移量无效。
   */
  def truncateTo(offset: Long): Unit

  /**
   * Remove all the entries from the index and resize the index to the max index size.
    * 从索引中删除所有条目，并将索引文件的大小调整为最大大小。
   */
  def reset(): Unit = {
    truncate()
    resize(maxIndexSize)
  }

  /**
   * Get offset relative to base offset of this index
    * 获取相对于此索引的baseOffset的偏移量   计算：offset - baseOffset
   * @throws IndexOffsetOverflowException
   */
  def relativeOffset(offset: Long): Int = {

    val relativeOffset = toRelative(offset)
    if (relativeOffset.isEmpty)
      throw new IndexOffsetOverflowException(s"Integer overflow for offset: $offset (${file.getAbsoluteFile})")
    relativeOffset.get

  }

  /**
   * Check if a particular offset is valid to be appended to this index.
    * 检查特定偏移量是否有效附加到此索引。
   * @param offset The offset to check
   * @return true if this offset is valid to be appended to this index; false otherwise如果此偏移量有效附加到此索引，则返回true;否则返回false。否则是假的
   */
  def canAppendOffset(offset: Long): Boolean = {
    toRelative(offset).isDefined
  }

  //安全强制取消映射
  protected def safeForceUnmap(): Unit = {
    try forceUnmap()
    catch {
      case t: Throwable => error(s"Error unmapping index $file", t)
    }
  }

  /**
   * Forcefully free the buffer's mmap.强制释放缓冲区的mmap。
   */
  protected[log] def forceUnmap() {
    try MappedByteBuffers.unmap(file.getAbsolutePath, mmap)
    finally mmap = null // Accessing unmapped mmap crashes JVM by SEGV so we null it out to be safe
  }

  /**
   * Execute the given function in a lock only if we are running on windows. We do this
   * because Windows won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
    * 仅当我们在Windows上运行时才在锁中执行给定的功能。我们这样做是因为Windows不会让我们在文件映射时调整文件大小。因此，我们必须强制取消映射，这需要同步读取。
   */
  protected def maybeLock[T](lock: Lock)(fun: => T): T = {
    if (OperatingSystem.IS_WINDOWS)
      lock.lock()
    try fun
    finally {
      if (OperatingSystem.IS_WINDOWS)
        lock.unlock()
    }
  }

  /**
   * To parse an entry in the index.
    * 解析索引中的条目。
   *
   * @param buffer the buffer of this memory mapped index.
   * @param n the slot
   * @return the index entry stored in the given slot.
   */
  protected def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry

  /**
   * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
   * The comparison is made using the `IndexEntry.compareTo()` method.
   *找到存储小于或等于给定目标键或值的最大条目的插槽。 使用`IndexEntry.compareTo（）`方法进行比较。
   * @param idx The index buffer
   * @param target The index key to look for
   * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
   */
  protected def largestLowerBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int =
    indexSlotRangeFor(idx, target, searchEntity)._1

  /**
   * Find the smallest entry greater than or equal the target key or value. If none can be found, -1 is returned.
    * 找到大于或等于目标键或值的最小条目。如果找不到，则返回-1。
   */
  protected def smallestUpperBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int =
    indexSlotRangeFor(idx, target, searchEntity)._2

  /**
   * Lookup lower and upper bounds for the given target.  这里是二分查找
    * 查找给定目标的下限和上限。
   */
  private def indexSlotRangeFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): (Int, Int) = {
    // 检查索引是否为空
    if(_entries == 0)
      return (-1, -1)

    // 检查目标偏移量是否小于最小偏移量
    if(compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0)
      return (-1, 0)

    // binary search for the entry 二分查找搜索条目
    var lo = 0
    var hi = _entries - 1
    while(lo < hi) {
      val mid = ceil(hi/2.0 + lo/2.0).toInt
      val found = parseEntry(idx, mid)
      val compareResult = compareIndexEntry(found, target, searchEntity)
      if(compareResult > 0)
        hi = mid - 1
      else if(compareResult < 0)
        lo = mid
      else
        return (mid, mid)
    }

    (lo, if (lo == _entries - 1) -1 else lo + 1)
  }
  // 比较索引条目，比较indexEntry的key、value和target
  private def compareIndexEntry(indexEntry: IndexEntry, target: Long, searchEntity: IndexSearchEntity): Int = {
    searchEntity match {
      case IndexSearchType.KEY => indexEntry.indexKey.compareTo(target)
      case IndexSearchType.VALUE => indexEntry.indexValue.compareTo(target)
    }
  }

  /**
    * 将数字舍入到给定因子的最大精确倍数，小于给定数字。
    * 例如roundDownToExactMultiple（67,8）== 64
    * 输出factor比number小的最大倍数
   */
  private def roundDownToExactMultiple(number: Int, factor: Int) = factor * (number / factor)

  //offset - baseOffset
  private def toRelative(offset: Long): Option[Int] = {
    val relativeOffset = offset - baseOffset
    if (relativeOffset < 0 || relativeOffset > Int.MaxValue)
      None
    else
      Some(relativeOffset.toInt)
  }

}

object IndexSearchType extends Enumeration {
  // 索引搜索实体
  type IndexSearchEntity = Value
  val KEY, VALUE = Value
}
