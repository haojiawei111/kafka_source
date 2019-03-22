/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.memory;

import java.nio.ByteBuffer;


/**
 * A common memory pool interface for non-blocking pools.用于非阻塞池的公共内存池接口。
 * Every buffer returned from {@link #tryAllocate(int)} must always be {@link #release(ByteBuffer) released}.
 */
public interface MemoryPool {

    MemoryPool NONE = new MemoryPool() {
        @Override
        public ByteBuffer tryAllocate(int sizeBytes) {
            return ByteBuffer.allocate(sizeBytes);
        }

        @Override
        public void release(ByteBuffer previouslyAllocated) {
            //nop
        }
        //long MAX_VALUE = 0x7fffffffffffffffL
        // 内存池大小
        @Override
        public long size() {
            return Long.MAX_VALUE;
        }

        // 可用内存
        @Override
        public long availableMemory() {
            return Long.MAX_VALUE;
        }

        @Override
        public boolean isOutOfMemory() {
            return false;
        }

        @Override
        public String toString() {
            return "NONE";
        }
    };

    /**
     * 试图获取指定大小的ByteBuffer。
     * @param sizeBytes size required
     * @return a ByteBuffer (which later needs to be release()ed), or null if no memory available.
     *         the buffer will be of the exact size requested, even if backed by a larger chunk of memory
     */
    ByteBuffer tryAllocate(int sizeBytes);

    /**
     * Returns a previously allocated buffer to the pool. 返回先前为池分配的缓冲区。
     * @param previouslyAllocated a buffer previously returned from tryAllocate()
     */
    void release(ByteBuffer previouslyAllocated);

    /**
     * 返回此池的总大小
     * @return total size, in bytes
     */
    long size();

    /**
     * 返回此池可用于分配的内存量。
     * NOTE: result may be negative (pools may over allocate to avoid starvation issues) 结果可能是负数（池可能过度分配以避免饥饿问题）
     * @return bytes available
     */
    long availableMemory();

    /**
     * Returns true if the pool cannot currently allocate any more buffers
     * - meaning total outstanding buffers meets or exceeds pool size and
     * some would need to be released before further allocations are possible.
     * 如果池当前无法分配任何更多缓冲区，则返回true *  - 表示总未完成缓冲区满足或超过池大小*在需要进一步分配之前需要释放一些缓冲区。
     *
     * 这相当于availableMemory（）<= 0
     * @return true if out of memory
     */
    boolean isOutOfMemory();
}
