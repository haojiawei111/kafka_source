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
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

/**
 * Provides access to the {@link QueryableStoreType}s provided with KafkaStreams. These
 * can be used with {@link org.apache.kafka.streams.KafkaStreams#store(String, QueryableStoreType)}
 * To access and query the {@link StateStore}s that are part of a Topology
 * 提供对KafkaStreams提供的{@link QueryableStoreType}的访问。
 * 这些可以与{@link org.apache.kafka.streams.KafkaStreams #store（String，QueryableStoreType）}一起使用访问和查询作为拓扑一部分的{@link StateStore}
 *
 * 三个静态方法创建不同的 KeyValueStoreType WindowStoreType SessionStoreType
 * 通过KeyValueStoreType WindowStoreType SessionStoreType可以创建出CompositeReadOnlyKeyValueStore CompositeReadOnlyWindowStore CompositeReadOnlySessionStore
 * 通过CompositeReadOnlyKeyValueStore CompositeReadOnlyWindowStore CompositeReadOnlySessionStore里面包装的状态提供者可以对状态进行只读查询
 *
 */
public class QueryableStoreTypes {

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyKeyValueStore}
     *
     * 创建一个KeyValueStoreType类型对象
     *
     * @param <K>   key type of the store
     * @param <V>   value type of the store
     * @return  {@link QueryableStoreTypes.KeyValueStoreType}
     */
    public static <K, V> QueryableStoreType<ReadOnlyKeyValueStore<K, V>> keyValueStore() {
        return new KeyValueStoreType<>();
    }

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyWindowStore}
     *
     * 创建一个WindowStoreType类型对象
     *
     * @param <K>   key type of the store
     * @param <V>   value type of the store
     * @return  {@link QueryableStoreTypes.WindowStoreType}
     */
    public static <K, V> QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStore() {
        return new WindowStoreType<>();
    }

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlySessionStore}
     *
     * 创建一个SessionStoreType类型对象
     *
     *
     * @param <K>   key type of the store
     * @param <V>   value type of the store
     * @return  {@link QueryableStoreTypes.SessionStoreType}
     */
    public static <K, V> QueryableStoreType<ReadOnlySessionStore<K, V>> sessionStore() {
        return new SessionStoreType<>();
    }

    private static abstract class QueryableStoreTypeMatcher<T> implements QueryableStoreType<T> {

        private final Class matchTo;

        QueryableStoreTypeMatcher(Class matchTo) {
            this.matchTo = matchTo;
        }

        // 是用来判断一个类Class1和另一个类Class2是否相同或是另一个类的超类或接口。
        // 检查StateStore是否是matchTo的子类，如果是就返回true，否则返回false
        @SuppressWarnings("unchecked")
        @Override
        public boolean accepts(final StateStore stateStore) {
            return matchTo.isAssignableFrom(stateStore.getClass());
        }
    }

    private static class KeyValueStoreType<K, V> extends QueryableStoreTypeMatcher<ReadOnlyKeyValueStore<K, V>> {
        KeyValueStoreType() {
            super(ReadOnlyKeyValueStore.class);
        }

        // 创建CompositeReadOnlyKeyValueStore，赋值StateStoreProvider和storeName
        @Override
        public ReadOnlyKeyValueStore<K, V> create(final StateStoreProvider storeProvider,
                                                  final String storeName) {
            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
        }

    }

    private static class WindowStoreType<K, V> extends QueryableStoreTypeMatcher<ReadOnlyWindowStore<K, V>> {
        WindowStoreType() {
            super(ReadOnlyWindowStore.class);
        }

        // 创建CompositeReadOnlyWindowStore，赋值StateStoreProvider和storeName
        @Override
        public ReadOnlyWindowStore<K, V> create(final StateStoreProvider storeProvider,
                                                final String storeName) {
            return new CompositeReadOnlyWindowStore<>(storeProvider, this, storeName);
        }
    }

    private static class SessionStoreType<K, V> extends QueryableStoreTypeMatcher<ReadOnlySessionStore<K, V>> {
        SessionStoreType() {
            super(ReadOnlySessionStore.class);
        }
        // 创建CompositeReadOnlySessionStore，赋值StateStoreProvider和storeName
        @Override
        public ReadOnlySessionStore<K, V> create(final StateStoreProvider storeProvider, final String storeName) {
            return new CompositeReadOnlySessionStore<>(storeProvider, this, storeName);
        }
    }
}
