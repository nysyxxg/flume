/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.node;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.CounterGroup;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class PollingPropertiesFileConfigurationProvider
        extends PropertiesFileConfigurationProvider
        implements LifecycleAware {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PollingPropertiesFileConfigurationProvider.class);

    private final EventBus eventBus;
    private final File file;
    private final int interval;
    private final CounterGroup counterGroup;
    private LifecycleState lifecycleState;

    private ScheduledExecutorService executorService;

    public PollingPropertiesFileConfigurationProvider(String agentName,
                                                      File file, EventBus eventBus, int interval) {
        super(agentName, file);
        this.eventBus = eventBus;
        this.file = file;
        this.interval = interval;
        counterGroup = new CounterGroup();
        lifecycleState = LifecycleState.IDLE;
    }

    @Override
    public void start() {
        LOGGER.info("Configuration provider starting");

        Preconditions.checkState(file != null, "The parameter file must not be null");

        /**
         * PollingPropertiesFileConfigurationProvider类中，我们发现在start()方法中，
         * new了一个单线程执行器Executors.newSingleThreadScheduledExecutor(),然后每隔30s（interval=30s，
         * Application类调用的时候传入）调度执行一次FileWatcherRunnable任务。
         */
        executorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("conf-file-poller-%d").build());

        /**
         * 主要作用：监控配置文件
         *   新启动一个线程，监控到有文件变动就将getConfiguration加到eventBus中，eventBus有事件更新会调用Application类中
         *   用@Subscribe修饰的函数，也就是 public void handleConfigurationEvent(MaterializedConfiguration conf)
         *   eventBus.post(getConfiguration())将conf对象通过总线传给了handleConfigurationEvent去处理
         */
        FileWatcherRunnable fileWatcherRunnable = new FileWatcherRunnable(file, counterGroup);

        executorService.scheduleWithFixedDelay(fileWatcherRunnable, 0, interval,
                TimeUnit.SECONDS);

        lifecycleState = LifecycleState.START;

        LOGGER.debug("Configuration provider started");
    }

    @Override
    public void stop() {
        LOGGER.info("Configuration provider stopping");

        executorService.shutdown();
        try {
            while (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                LOGGER.debug("Waiting for file watcher to terminate");
            }
        } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while waiting for file watcher to terminate");
            Thread.currentThread().interrupt();
        }
        lifecycleState = LifecycleState.STOP;
        LOGGER.debug("Configuration provider stopped");
    }

    @Override
    public synchronized LifecycleState getLifecycleState() {
        return lifecycleState;
    }


    @Override
    public String toString() {
        return "{ file:" + file + " counterGroup:" + counterGroup + "  provider:"
                + getClass().getCanonicalName() + " agentName:" + getAgentName() + " }";
    }

    /**监控flume的配置文件，是否发生了改变
     * FileWatcherRunnable任务用于监控配置文件的变化
     */
    public class FileWatcherRunnable implements Runnable {

        private final File file;
        private final CounterGroup counterGroup;

        private long lastChange;

        public FileWatcherRunnable(File file, CounterGroup counterGroup) {
            super();
            this.file = file;
            this.counterGroup = counterGroup;
            this.lastChange = 0L;
        }

        @Override
        public void run() {
            LOGGER.debug("Checking file:{} for changes", file);

            counterGroup.incrementAndGet("file.checks");

            long lastModified = file.lastModified();// 最后的修改时间

            // 如果配置文件发生变化，则调用eventBus.post(getConfiguration())语句将事件发送到eventBus主线，
            // eventBus负责调用观察者（Application）调用事件处理函数（handleConfigurationEvent(MaterializedConfiguration conf)）处理事件
            if (lastModified > lastChange) {// 其实在第一次启动的时候，lastModified和lastChange这两个值是不相等的，
                LOGGER.info("Reloading configuration file:{}", file);

                counterGroup.incrementAndGet("file.loads");

                lastChange = lastModified;

                try {
                   // eventBus.post(getConfiguration())将conf对象通过总线传给了handleConfigurationEvent去处理
                    eventBus.post(getConfiguration()); // 提交文件的配置信息
                } catch (Exception e) {
                    LOGGER.error("Failed to load configuration data. Exception follows.",
                            e);
                } catch (NoClassDefFoundError e) {
                    LOGGER.error("Failed to start agent because dependencies were not " +
                            "found in classpath. Error follows.", e);
                } catch (Throwable t) {
                    // caught because the caller does not handle or log Throwables
                    LOGGER.error("Unhandled error", t);
                }
            }
        }
    }

}
