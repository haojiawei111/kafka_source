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

package kafka.common

/**
  * A mutable cell that holds a value of type `Long`. One should generally prefer using value-based programming (i.e.
  * passing and returning `Long` values), but this class can be useful in some scenarios.
  *
  * Unlike `AtomicLong`, this class is not thread-safe and there are no atomicity guarantees.
  *
  * 一个可变单元格，其值为“Long”类型。通常应该更喜欢使用基于值的编程（即*传递和返回`Long`值），但是这个类在某些场景中很有用。
  * 与`AtomicLong`不同，此类不是线程安全的，并且没有原子性保证
  */
class LongRef(var value: Long) {

  def addAndGet(delta: Long): Long = {
    value += delta
    value
  }

  def getAndAdd(delta: Long): Long = {
    val result = value
    value += delta
    result
  }

  def getAndIncrement(): Long = {
    val v = value
    value += 1
    v
  }

  def incrementAndGet(): Long = {
    value += 1
    value
  }

  def getAndDecrement(): Long = {
    val v = value
    value -= 1
    v
  }

  def decrementAndGet(): Long = {
    value -= 1
    value
  }

}
