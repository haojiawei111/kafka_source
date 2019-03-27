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

import java.util.Arrays
import java.security.MessageDigest
import java.nio.ByteBuffer
import kafka.utils._
import org.apache.kafka.common.utils.Utils

// OffsetMap接口
trait OffsetMap {
  def slots: Int
  def put(key: ByteBuffer, offset: Long)
  def get(key: ByteBuffer): Long
  def updateLatestOffset(offset: Long)
  def clear()
  def size: Int
  def utilization: Double = size.toDouble / slots
  def latestOffset: Long
}

/**
 * An hash table used for deduplicating the log. This hash table uses a cryptographicly secure hash of the key as a proxy for the key
 * for comparisons and to save space on object overhead. Collisions are resolved by probing. This hash table does not support deletes.
  * 用于对日志进行重复数据删除的哈希表。此哈希表使用密钥的加密安全散列作为密钥的代理进行比较，并节省对象开销的空间。通过探测解决冲突。此哈希表不支持删除
 * @param memory The amount of memory this map can use 此映射可以使用的内存量
 * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512要使用的哈希算法实例：MD2，MD5，SHA-1，SHA-256，SHA-384，SHA-512
  *
  *这个类提供了一个在内存中的一段空间来删除重复额key        日志压缩
 */
@nonthreadsafe
class SkimpyOffsetMap(val memory: Int, val hashAlgorithm: String = "MD5") extends OffsetMap {

  private val bytes : ByteBuffer = ByteBuffer.allocate(memory)
  
  /* the hash algorithm instance to use, default is MD5 要使用的哈希算法实例，默认为MD5*/
  private val digest = MessageDigest.getInstance(hashAlgorithm)
  
  /* the number of bytes for this hash algorithm 此哈希算法的字节数 */
  private val hashSize = digest.getDigestLength
  
  /* create some hash buffers to avoid reallocating each time 创建一些哈希缓冲区以避免每次重新分配*/
  private val hash1 = new Array[Byte](hashSize)
  private val hash2 = new Array[Byte](hashSize)
  
  /* number of entries put into the map 放入map的条目数*/
  private var entries = 0
  
  /* number of lookups on the map map上的查找次数*/
  private var lookups = 0L
  
  /* the number of probes for all lookups 所有查找的探测数量*/
  private var probes = 0L

  /* the latest offset written into the map 写入map的最新偏移量*/
  private var lastOffset = -1L

  /**
   * The number of bytes of space each entry uses (the number of bytes in the hash plus an 8 byte offset)
    * 每个条目使用的空间字节数（散列中的字节数加上8字节的偏移量）
   */
  val bytesPerEntry = hashSize + 8
  
  /**
   * The maximum number of entries this map can contain
    * 此map可包含的最大条目数
   */
  val slots: Int = memory / bytesPerEntry
  
  /**
   * Associate this offset to the given key.
   * @param key The key
   * @param offset The offset
   */
  override def put(key: ByteBuffer, offset: Long) {
    // 放入map的条目数 < 此map可包含的最大条目数
    require(entries < slots, "Attempt to add a new entry to a full offset map.尝试将新条目添加到完整偏移映射")

    //map上的查找次数加一
    lookups += 1

    hashInto(key, hash1)
    // probe until we find the first empty slot 探测到我们找到第一个空槽
    var attempt = 0
    var pos = positionOf(hash1, attempt)

    while(!isEmpty(pos)) {
      bytes.position(pos)
      bytes.get(hash2)
      if(Arrays.equals(hash1, hash2)) {
        // we found an existing entry, overwrite it and return (size does not change)
        bytes.putLong(offset)
        lastOffset = offset
        return
      }
      attempt += 1
      pos = positionOf(hash1, attempt)
    }
    // found an empty slot, update it--size grows by 1 找到一个空槽，更新它 - 大小增加1
    // 移动到位置
    bytes.position(pos)
    // hash1 + offset
    bytes.put(hash1)
    bytes.putLong(offset)
    lastOffset = offset
    entries += 1
  }
  
  /**
   * Check that there is no entry at the given position
    * 检查给定位置是否没有条目
   */
  private def isEmpty(position: Int): Boolean = 
    bytes.getLong(position) == 0 && bytes.getLong(position + 8 ) == 0 && bytes.getLong(position + 16) == 0

  /**
   * Get the offset associated with this key.
    * 获取与此key关联的偏移量。
   * @param key The key
   * @return The offset associated with this key or -1 if the key is not found
   */
  override def get(key: ByteBuffer): Long = {
    lookups += 1
    hashInto(key, hash1)
    // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
    var attempt = 0
    var pos = 0
    //we need to guard against attempt integer overflow if the map is full
    //limit attempt to number of slots once positionOf(..) enters linear search mode
    val maxAttempts = slots + hashSize - 4
    do {
     if(attempt >= maxAttempts)
        return -1L
      pos = positionOf(hash1, attempt)
      bytes.position(pos)
      if(isEmpty(pos))
        return -1L
      bytes.get(hash2)
      attempt += 1
    } while(!Arrays.equals(hash1, hash2))
    bytes.getLong()
  }
  
  /**
   * Change the salt used for key hashing making all existing keys unfindable.
    * 更改用于密钥散列的salt，使所有现有密钥不可用。
   */
  override def clear() {
    this.entries = 0
    this.lookups = 0L
    this.probes = 0L
    this.lastOffset = -1L
    Arrays.fill(bytes.array, bytes.arrayOffset, bytes.arrayOffset + bytes.limit(), 0.toByte)
  }
  
  /**
   * The number of entries put into the map (note that not all may remain)
    * 放入may的条目数（请注意，并非所有条目都可以保留）
   */
  override def size: Int = entries
  
  /**
   * The rate of collisions in the lookups
    * 查找中的冲突率
   */
  def collisionRate: Double = 
    (this.probes - this.lookups) / this.lookups.toDouble

  /**
   * The latest offset put into the map
    * 拿到map中最新的偏移量放入
   */
  override def latestOffset: Long = lastOffset
  //更新map中最新的偏移量放入
  override def updateLatestOffset(offset: Long): Unit = {
    lastOffset = offset
  }

  /**
   * Calculate the ith probe position. We first try reading successive integers from the hash itself
   * then if all of those fail we degrade to linear probing.
    * 计算第i个探头位置。我们首先尝试从哈希本身读取连续的整数*然后如果所有这些都失败我们降级为线性探测。
   * @param hash The hash of the key to find the position for
   * @param attempt The ith probe
   * @return The byte offset in the buffer at which the ith probing for the given hash would reside
   */

  private def positionOf(hash: Array[Byte], attempt: Int): Int = {
    //探测
    val probe = CoreUtils.readInt(hash, math.min(attempt, hashSize - 4)) + math.max(0, attempt - hashSize + 4)

    //插槽
    val slot = Utils.abs(probe) % slots

    // 探测加一
    this. probes += 1

    slot * bytesPerEntry
  }
  
  /**
   * The offset at which we have stored the given key我们存储给定键的偏移量
   * @param key The key to hash
   * @param buffer The buffer to store the hash into用于存储哈希的缓冲区
   */
  private def hashInto(key: ByteBuffer, buffer: Array[Byte]) {
    // 通过mark方法可以标记当前的position
    key.mark()
    // 使用 update（）方法处理数据   使用指定的 byte 数组更新摘要。
    digest.update(key)
    // 通过reset来恢复mark的位置
    key.reset()
    //调用digest() 方法之一完成哈希计算。 通过执行诸如填充之类的最终操作完成哈希计算。在调用此方法之后，摘要被重置。
    digest.digest(buffer, 0, hashSize)
  }
  
}
