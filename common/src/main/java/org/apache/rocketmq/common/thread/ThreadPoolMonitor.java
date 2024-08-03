/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.thread;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class ThreadPoolMonitor {
    private static Logger jstackLogger = LoggerFactory.getLogger(ThreadPoolMonitor.class);
    private static Logger waterMarkLogger = LoggerFactory.getLogger(ThreadPoolMonitor.class);

    private static final List<ThreadPoolWrapper> MONITOR_EXECUTOR = new CopyOnWriteArrayList<>();
    private static final ScheduledExecutorService MONITOR_SCHEDULED = ThreadUtils.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("ThreadPoolMonitor-%d").build()
    );

    private static volatile long threadPoolStatusPeriodTime = TimeUnit.SECONDS.toMillis(3);
    private static volatile boolean enablePrintJstack = true;
    private static volatile long jstackPeriodTime = 60000;
    private static volatile long jstackTime = System.currentTimeMillis();

    public static void config(Logger jstackLoggerConfig, Logger waterMarkLoggerConfig,
        boolean enablePrintJstack, long jstackPeriodTimeConfig, long threadPoolStatusPeriodTimeConfig) {
        jstackLogger = jstackLoggerConfig;
        waterMarkLogger = waterMarkLoggerConfig;
        threadPoolStatusPeriodTime = threadPoolStatusPeriodTimeConfig;
        ThreadPoolMonitor.enablePrintJstack = enablePrintJstack;
        jstackPeriodTime = jstackPeriodTimeConfig;
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        int queueCapacity) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, queueCapacity, Collections.emptyList());
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        int queueCapacity,
        ThreadPoolStatusMonitor... threadPoolStatusMonitors) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, queueCapacity,
            Lists.newArrayList(threadPoolStatusMonitors));
    }

    /**
     * 创建并监控一个线程池执行器
     *
     * @param corePoolSize 核心线程池大小
     * @param maximumPoolSize 最大线程池大小
     * @param keepAliveTime 空闲线程存活时间
     * @param unit 时间单位
     * @param name 线程池名称
     * @param queueCapacity 队列容量
     * @param threadPoolStatusMonitors 线程池状态监控器列表
     * @return 创建的线程池执行器
     */
    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        int queueCapacity,
        List<ThreadPoolStatusMonitor> threadPoolStatusMonitors) {
        // 创建线程池执行器
        ThreadPoolExecutor executor = (ThreadPoolExecutor) ThreadUtils.newThreadPoolExecutor(
            corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            unit,
            new LinkedBlockingQueue<>(queueCapacity),
            new ThreadFactoryBuilder().setNameFormat(name + "-%d").build(),
            new ThreadPoolExecutor.DiscardOldestPolicy());

        // 初始化监控器列表，添加队列大小监控器
        List<ThreadPoolStatusMonitor> printers = Lists.newArrayList(new ThreadPoolQueueSizeMonitor(queueCapacity));
        // 将传入的监控器添加到列表中
        printers.addAll(threadPoolStatusMonitors);

        // 将线程池及其监控器添加到监控执行器中
        MONITOR_EXECUTOR.add(ThreadPoolWrapper.builder()
            .name(name)
            .threadPoolExecutor(executor)
            .statusPrinters(printers)
            .build());
        // 返回创建的线程池执行器
        return executor;
    }


    /**
     * 记录线程池状态
     * 遍历所有监控的线程池，获取并记录其状态信息
     * 如果开启打印JVM堆栈信息且满足条件，则记录JVM堆栈信息
     */
    public static void logThreadPoolStatus() {
        // 遍历所有监控的线程池
        for (ThreadPoolWrapper threadPoolWrapper : MONITOR_EXECUTOR) {
            // 获取线程池状态监视器列表
            List<ThreadPoolStatusMonitor> monitors = threadPoolWrapper.getStatusPrinters();
            // 遍历所有监视器
            for (ThreadPoolStatusMonitor monitor : monitors) {
                // 获取线程池的状态值
                double value = monitor.value(threadPoolWrapper.getThreadPoolExecutor());
                // 格式化线程池名称
                String nameFormatted = String.format("%-40s", threadPoolWrapper.getName());
                // 格式化状态描述
                String descFormatted = String.format("%-12s", monitor.describe());
                // 记录线程池状态信息
                waterMarkLogger.info("{}{}{}", nameFormatted, descFormatted, value);
                // 如果开启打印JVM堆栈信息
                if (enablePrintJstack) {
                    // 判断是否需要打印JVM堆栈信息，并检查是否超过了设定的间隔时间
                    if (monitor.needPrintJstack(threadPoolWrapper.getThreadPoolExecutor(), value) &&
                        System.currentTimeMillis() - jstackTime > jstackPeriodTime) {
                        // 更新JVM堆栈信息打印时间
                        jstackTime = System.currentTimeMillis();
                        // 记录JVM堆栈信息
                        jstackLogger.warn("jstack start\n{}", UtilAll.jstack());
                    }
                }
            }
        }
    }


    /**
     * 初始化线程池监控任务调度
     * 该方法使用定时调度器（MONITOR_SCHEDULED）来定期记录线程池的状态
     * 首次执行延迟20毫秒，之后按照设定的时间间隔重复执行
     *
     * @see #MONITOR_SCHEDULED 调度器用于执行监控任务
     * @see ThreadPoolMonitor#logThreadPoolStatus() 监控任务的具体执行方法
     * @see #threadPoolStatusPeriodTime 监控任务执行的间隔时间
     */
    public static void init() {
        MONITOR_SCHEDULED.scheduleAtFixedRate(ThreadPoolMonitor::logThreadPoolStatus, 20,
            threadPoolStatusPeriodTime, TimeUnit.MILLISECONDS);
    }


    public static void shutdown() {
        MONITOR_SCHEDULED.shutdown();
    }
}