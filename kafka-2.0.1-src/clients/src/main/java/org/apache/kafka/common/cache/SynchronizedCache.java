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
package org.apache.kafka.common.cache;

/**
 * Wrapper for caches that adds simple synchronization to provide a thread-safe cache. Note that this simply adds
 * synchronization around each cache method on the underlying unsynchronized cache. It does not add any support for
 * atomically checking for existence of an entry and computing and inserting the value if it is missing.
 * 装饰者模式
 * 缓存的包装器，它添加简单的同步以提供线程安全的缓存。
 * 注意，这只是在底层的非同步缓存周围添加了每个缓存方法的同步。
 * 它不添加任何支持自动检查条目是否存在，以及计算和插入缺失的值。
 */
public class SynchronizedCache<K, V> implements Cache<K, V> {
    private final Cache<K, V> underlying;

    public SynchronizedCache(Cache<K, V> underlying) {
        this.underlying = underlying;
    }

    @Override
    public synchronized V get(K key) {
        return underlying.get(key);
    }

    @Override
    public synchronized void put(K key, V value) {
        underlying.put(key, value);
    }

    @Override
    public synchronized boolean remove(K key) {
        return underlying.remove(key);
    }

    @Override
    public synchronized long size() {
        return underlying.size();
    }
}
