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
package org.apache.kafka.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for Thread that sets things up nicely
 * Thread的包装器 可以很好地设置线程
 */
public class KafkaThread extends Thread {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    public static KafkaThread daemon(final String name, Runnable runnable) {
        return new KafkaThread(name, runnable, true);
    }

    public static KafkaThread nonDaemon(final String name, Runnable runnable) {
        return new KafkaThread(name, runnable, false);
    }

    public KafkaThread(final String name, boolean daemon) {
        super(name);
        configureThread(name, daemon);
    }

    public KafkaThread(final String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        configureThread(name, daemon);
    }

    private void configureThread(final String name, boolean daemon) {
        //thread.setDaemon(true)必须在thread.start()之前设置，否则会跑出一个IllegalThreadStateException异常。你不能把正在运行的常规线程设置为守护线程。
        setDaemon(daemon);
        //当一个线程由于发生了非受检异常而终止时，JVM会使用Thread.gerUncaughtExceptionHandler()方法查看该线程上的UncaughtExceptionHandler，并调用他的uncaughtException()方法

// 如果一个线程没有显式的设置它的UncaughtExceptionHandler，JVM就会检查该线程所在的线程组是否设置了UncaughtExceptionHandler，如果已经设置，就是用该UncaughtExceptionHandler；
// 否则查看是否在Thread层面通过静态方法setDefaultUncaughtExceptionHandler()设置了UncaughtExceptionHandler，如果已经设置就是用该UncaughtExceptionHandler；如果上述都没有找到，
// JVM会在对应的console中打印异常的堆栈信息。

        setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                log.error("Uncaught exception in thread '{}':", name, e);
            }
        });
    }

}
