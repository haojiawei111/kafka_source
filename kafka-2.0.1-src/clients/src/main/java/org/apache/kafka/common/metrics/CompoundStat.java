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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;

import java.util.List;

/**
 * A compound stat is a stat where a single measurement and associated data structure feeds many metrics. This is the
 * example for a histogram which has many associated percentiles.
 * 复合统计是一种统计，其中单个度量和关联数据结构提供许多度量。这是具有许多相关百分位数的直方图的*示例。
 */
public interface CompoundStat extends Stat {

    public List<NamedMeasurable> stats();

    // 静态内部类
    public static class NamedMeasurable {

        private final MetricName name;
        private final Measurable stat;

        public NamedMeasurable(MetricName name, Measurable stat) {
            super();
            this.name = name;
            this.stat = stat;
        }

        public MetricName name() {
            return name;
        }

        public Measurable stat() {
            return stat;
        }

    }

}
