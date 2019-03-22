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

package kafka.utils

import java.util.concurrent._

import org.apache.kafka.common.KafkaException

import collection.mutable
import collection.JavaConverters._

/**
  * 功能：简单的并发对象池
  * 实现对ConcurrentHashMap的封裝
  * getAndMaybePut实现小技巧, 使用了double check技术, 在有值的情况下降低锁的开销;
  *
  * @param valueFactory
  * @tparam K
  * @tparam V
  */

class Pool[K,V](valueFactory: Option[K => V] = None) extends Iterable[(K, V)] {

  // 线程安全的map
  private val pool: ConcurrentMap[K, V] = new ConcurrentHashMap[K, V]
  
  def put(k: K, v: V): V = pool.put(k, v)
  
  def putIfNotExists(k: K, v: V): V = pool.putIfAbsent(k, v)

  /**
    * 获取与给定键关联的值。如果没有关联的值，则使用池的值factory创建值，并返回与该键关联的值。如果需要避免副作用，则用户应将工厂方法声明为惰性。
   *
   * @param key The key to lookup.
   * @return The final value associated with the key.
   */
  def getAndMaybePut(key: K): V = {
    if (valueFactory.isEmpty)
      throw new KafkaException("Empty value factory in pool 池中的空值工厂.")
    getAndMaybePut(key, valueFactory.get(key))
  }

  /**
    * 获取与给定key关联的值。如果没有关联的值，则使用`createValue`提供的值创建值，并返回与该key关联的值。
    *
    * @param key The key to lookup.
    * @param createValue Factory function.
    * @return The final value associated with the key.
    */
  def getAndMaybePut(key: K, createValue: => V): V =
    pool.computeIfAbsent(key, new java.util.function.Function[K, V] {
      override def apply(k: K): V = createValue
    })

  def contains(id: K): Boolean = pool.containsKey(id)
  
  def get(key: K): V = pool.get(key)
  
  def remove(key: K): V = pool.remove(key)

  def remove(key: K, value: V): Boolean = pool.remove(key, value)

  def keys: mutable.Set[K] = pool.keySet.asScala

  def values: Iterable[V] = pool.values.asScala

  def clear() { pool.clear() }
  
  override def size: Int = pool.size
  
  override def iterator: Iterator[(K, V)] = new Iterator[(K,V)]() {
    
    private val iter = pool.entrySet.iterator
    
    def hasNext: Boolean = iter.hasNext
    
    def next: (K, V) = {
      val n = iter.next
      (n.getKey, n.getValue)
    }
    
  }
    
}
