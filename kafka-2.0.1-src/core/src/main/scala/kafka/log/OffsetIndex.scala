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

import java.io.File
import java.nio.ByteBuffer

import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.errors.InvalidOffsetException

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 *
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 *
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 *
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 *
 * The frequency of entries is up to the user of this class.
 *
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
  * 将偏移量映射到特定日志段的物理文件位置的索引。这个索引可能很稀疏：
  * 也就是说它可能没有为日志中的所有消息保留条目。索引存储在预先分配的文件中，以保存固定的最大数量的8字节条目。
  * 索引支持针对此文件的内存映射进行查找。这些查找使用简单的二进制搜索变体完成
  * 找到偏移/位置对的最大偏移量小于或等于目标偏移量。
  * 索引文件可以通过两种方式打开：作为允许追加的空的可变索引或
  * 一个先前已填充的不可变只读索引文件。 makeReadOnly方法将可变文件转换为不可变文件并截断​​任何额外字节。这是在转换索引文件时完成的。
  * 在重建崩溃的情况下，不会尝试校验此文件的内容。
  * 文件格式是一系列条目。物理格式是4字节“相对”偏移量和4字节文件位置
  * 带有该偏移量的消息。存储的偏移量相对于索引文件的基本偏移量。所以，例如，
  * 如果基本偏移量为50，那么偏移量55将被存储为5.以这种方式使用相对偏移量，让我们仅使用4个字节作为偏移量。
  * 条目的频率取决于该类的用户。所有外部API都从相对偏移转换为完全偏移，因此此类的用户不与内部存储格式交互。
 */
// Avoid shadowing mutable `file` in AbstractIndex 避免在AbstractIndex中隐藏可变的`file`
class OffsetIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true) extends AbstractIndex[Long, Int](_file, baseOffset, maxIndexSize, writable) {

  override def entrySize = 8

  /* the last offset in the index 索引中的最后一个偏移量 */
  private[this] var _lastOffset = lastEntry.offset

  debug("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d"
    .format(file.getAbsolutePath, maxEntries, maxIndexSize, _entries, _lastOffset, mmap.position()))

  /**
   * The last entry in the index 索引中的最后一个条目
   */
  private def lastEntry: OffsetPosition = {
    inLock(lock) {
      _entries match {
        case 0 => OffsetPosition(baseOffset, 0)
        case s => parseEntry(mmap, s - 1).asInstanceOf[OffsetPosition]
      }
    }
  }

  def lastOffset: Long = _lastOffset

  /**
    * =查找小于等于指定 offset 的最大 offset,并且返回对应的 offset 和实际物理位置
   *
   * @param targetOffset The offset to look up.
   * @return The offset found and the corresponding file position for this offset
   *         If the target offset is smaller than the least entry in the index (or the index is empty),
   *         the pair (baseOffset, 0) is returned.
   */
  // offset 索引文件是使用内存映射的方式加载到内存中的，
  // 在查询的过程中，内存映射是会发生变化，所以在 lookup() 中先拷贝出来了一个（idx），然后再进行查询，具体实现如下：
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      //mmap是一个Buffer类
      //duplicate复制当前的ByteBuffer
      val idx: ByteBuffer = mmap.duplicate //note: 查询时,mmap 会发生变化,先复制出来一个
      val slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY)
      if(slot == -1)
        OffsetPosition(baseOffset, 0)
      else
      //note: 先计算绝对偏移量,再计算物理位置
        parseEntry(idx, slot).asInstanceOf[OffsetPosition]
    }
  }

  /**
   * Find an upper bound offset for the given fetch starting position and size. This is an offset which
   * is guaranteed to be outside the fetched range, but note that it will not generally be the smallest
   * such offset.
    * 找到给定的提取起始位置和大小的上限偏移量。这是一个偏移，保证在取出的范围之外，但请注意，它通常不会是最小的这样的偏移。
   */
  def fetchUpperBoundOffset(fetchOffset: OffsetPosition, fetchSize: Int): Option[OffsetPosition] = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = smallestUpperBoundSlotFor(idx, fetchOffset.position + fetchSize, IndexSearchType.VALUE)
      if (slot == -1)
        None
      else
        Some(parseEntry(idx, slot).asInstanceOf[OffsetPosition])
    }
  }

  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize)

  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 4)

  override def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry = {
      OffsetPosition(baseOffset + relativeOffset(buffer, n), physical(buffer, n))
  }

  /**
   * Get the nth offset mapping from the index
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException("Attempt to fetch the %dth entry from an index of size %d.".format(n, _entries))
      val idx = mmap.duplicate
      OffsetPosition(relativeOffset(idx, n), physical(idx, n))
    }
  }

  /**
   * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
   * @throws IndexOffsetOverflowException if the offset causes index offset to overflow
   */
  def append(offset: Long, position: Int) {
    inLock(lock) {
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      if (_entries == 0 || offset > _lastOffset) {
        debug("Adding index entry %d => %d to %s.".format(offset, position, file.getName))
        mmap.putInt(relativeOffset(offset))
        mmap.putInt(position)
        _entries += 1
        _lastOffset = offset
        require(_entries * entrySize == mmap.position(), entries + " entries but file position in index is " + mmap.position() + ".")
      } else {
        throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s."
          .format(offset, entries, _lastOffset, file.getAbsolutePath))
      }
    }
  }

  override def truncate() = truncateToEntries(0)

  override def truncateTo(offset: Long) {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.KEY)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries =
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  /**
   * Truncates index to a known number of entries.
    * 将索引截断为已知数量的条目。
   */
  private def truncateToEntries(entries: Int) {
    inLock(lock) {
      _entries = entries
      mmap.position(_entries * entrySize)
      _lastOffset = lastEntry.offset
    }
  }

  override def sanityCheck() {
    if (_entries != 0 && _lastOffset < baseOffset)
      throw new CorruptIndexException(s"Corrupt index found, index file (${file.getAbsolutePath}) has non-zero size " +
        s"but the last offset is ${_lastOffset} which is less than the base offset $baseOffset.")
    if (length % entrySize != 0)
      throw new CorruptIndexException(s"Index file ${file.getAbsolutePath} is corrupt, found $length bytes which is " +
        s"neither positive nor a multiple of $entrySize.")
  }

}
