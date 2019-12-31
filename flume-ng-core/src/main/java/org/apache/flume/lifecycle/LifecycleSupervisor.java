/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.lifecycle;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LifecycleSupervisor implements LifecycleAware {

    private static final Logger logger = LoggerFactory.getLogger(LifecycleSupervisor.class);

    private Map<LifecycleAware, Supervisoree> supervisedProcesses;
    private Map<LifecycleAware, ScheduledFuture<?>> monitorFutures;

    private ScheduledThreadPoolExecutor monitorService;

    private LifecycleState lifecycleState;
    private Purger purger;
    private boolean needToPurge;

    public LifecycleSupervisor() {
        lifecycleState = LifecycleState.IDLE;
        supervisedProcesses = new HashMap<LifecycleAware, Supervisoree>();
        monitorFutures = new HashMap<LifecycleAware, ScheduledFuture<?>>();
        monitorService = new ScheduledThreadPoolExecutor(10,
                new ThreadFactoryBuilder().setNameFormat(
                        "lifecycleSupervisor-" + Thread.currentThread().getId() + "-%d")
                        .build());
        monitorService.setMaximumPoolSize(20);
        monitorService.setKeepAliveTime(30, TimeUnit.SECONDS);
        purger = new Purger();
        needToPurge = false;
    }

    @Override
    public synchronized void start() {

        logger.info("Starting lifecycle supervisor {}", Thread.currentThread()
                .getId());
        monitorService.scheduleWithFixedDelay(purger, 2, 2, TimeUnit.HOURS);
        lifecycleState = LifecycleState.START;

        logger.debug("Lifecycle supervisor started");
    }

    @Override
    public synchronized void stop() {

        logger.info("Stopping lifecycle supervisor {}", Thread.currentThread()
                .getId());

        if (monitorService != null) {
            monitorService.shutdown();
            try {
                monitorService.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for monitor service to stop");
            }
            if (!monitorService.isTerminated()) {
                monitorService.shutdownNow();
                try {
                    while (!monitorService.isTerminated()) {
                        monitorService.awaitTermination(10, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for monitor service to stop");
                }
            }
        }

        for (final Entry<LifecycleAware, Supervisoree> entry : supervisedProcesses.entrySet()) {

            if (entry.getKey().getLifecycleState().equals(LifecycleState.START)) {
                entry.getValue().status.desiredState = LifecycleState.STOP;
                entry.getKey().stop();
            }
        }

        /* If we've failed, preserve the error state. */
        if (lifecycleState.equals(LifecycleState.START)) {
            lifecycleState = LifecycleState.STOP;
        }
        supervisedProcesses.clear();
        monitorFutures.clear();
        logger.debug("Lifecycle supervisor stopped");
    }

    public synchronized void fail() {
        lifecycleState = LifecycleState.ERROR;
    }

    public synchronized void supervise(LifecycleAware lifecycleAware,
                                       SupervisorPolicy policy, LifecycleState desiredState) {
        if (this.monitorService.isShutdown()
                || this.monitorService.isTerminated()
                || this.monitorService.isTerminating()) {
            throw new FlumeException("Supervise called on " + lifecycleAware + " " +
                    "after shutdown has been initiated. " + lifecycleAware + " will not" + " be started");
        }

        Preconditions.checkState(!supervisedProcesses.containsKey(lifecycleAware),
                "Refusing to supervise " + lifecycleAware + " more than once");

        if (logger.isDebugEnabled()) {
            logger.debug("Supervising service:{} policy:{} desiredState:{}", new Object[]{lifecycleAware, policy, desiredState});
        }

        Supervisoree process = new Supervisoree();
        process.status = new Status();
        process.policy = policy;
        process.status.desiredState = desiredState;
        process.status.error = false;

        //监控线程
        MonitorRunnable monitorRunnable = new MonitorRunnable();
        monitorRunnable.lifecycleAware = lifecycleAware;
        monitorRunnable.supervisoree = process;
        monitorRunnable.monitorService = monitorService;

        supervisedProcesses.put(lifecycleAware, process);

        // 通过ScheduleWithFixedDelay延时调用任务monitorRunnable，任务执行完之后，等待3s继续调度执行。
        ScheduledFuture<?> future = monitorService.scheduleWithFixedDelay(
                monitorRunnable, 0, 3, TimeUnit.SECONDS);
        monitorFutures.put(lifecycleAware, future);
    }

    public synchronized void unsupervise(LifecycleAware lifecycleAware) {

        Preconditions.checkState(supervisedProcesses.containsKey(lifecycleAware),
                "Unaware of " + lifecycleAware + " - can not unsupervise");

        logger.debug("Unsupervising service:{}", lifecycleAware);

        synchronized (lifecycleAware) {
            Supervisoree supervisoree = supervisedProcesses.get(lifecycleAware);
            supervisoree.status.discard = true;
            this.setDesiredState(lifecycleAware, LifecycleState.STOP);
            logger.info("Stopping component: {}", lifecycleAware);
            lifecycleAware.stop();
        }
        supervisedProcesses.remove(lifecycleAware);
        //We need to do this because a reconfiguration simply unsupervises old
        //components and supervises new ones.
        monitorFutures.get(lifecycleAware).cancel(false);
        //purges are expensive, so it is done only once every 2 hours.
        needToPurge = true;
        monitorFutures.remove(lifecycleAware);
    }

    public synchronized void setDesiredState(LifecycleAware lifecycleAware,
                                             LifecycleState desiredState) {

        Preconditions.checkState(supervisedProcesses.containsKey(lifecycleAware),
                "Unaware of " + lifecycleAware + " - can not set desired state to "
                        + desiredState);

        logger.debug("Setting desiredState:{} on service:{}", desiredState,
                lifecycleAware);

        Supervisoree supervisoree = supervisedProcesses.get(lifecycleAware);
        supervisoree.status.desiredState = desiredState;
    }

    @Override
    public synchronized LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    public synchronized boolean isComponentInErrorState(LifecycleAware component) {
        return supervisedProcesses.get(component).status.error;

    }

    // 监控启动和停止线程

    /**
     * 下面我们总结一下整个flume的调用顺序。
     * Application->LifecycleSupervisor-(3s调度一次)>MonitorRunnable--->PollingPropertiesFileConfigurationProvider-(30s调度一次)>
     * FileWatcherRunnable->EventBus->Application
     * 期间我们看到一个调度器调度了另一个调度器，而且间隔几秒一次，为什么没有出现多个重复任务实例被调度起来？
     * supervisor.supervise(component,new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
     *
     * 我们看到在LifecycleSupervisor执行调度的时候传入了一个LifecycleState.START值，
     * 这个值便是下面代码（MonitorRunnable的run函数）中的desiredState：
     *
     * 实现lifecycleAware接口的PollingPropertiesFileConfigurationProvider类在首次调用start()函数的时候，
     * 就已经将lifecycleState的值变为START：lifecycleState = LifecycleState.START;
     * 所以调度器在之后的调度过程中，
     * 由于if (!lifecycleAware.getLifecycleState().equals( supervisoree.status.desiredState))if条件不成立，
     * 便不会有新的任务被调度起来。PollingPropertiesFileConfigurationProvider任务只有一个线程实例，
     * 又由于调度FileWatcherRunnable的是一个单线程调度器，FileWatcherRunnable任务也只有一个线程实例。
     * 同理，各个channel、source和sink也都没有重复实例被调度起来。
     */
    public static class MonitorRunnable implements Runnable {

        public ScheduledExecutorService monitorService;
        public LifecycleAware lifecycleAware;
        public Supervisoree supervisoree;

        @Override
        public void run() {
            logger.debug("checking process:{} supervisoree:{}", lifecycleAware, supervisoree);

            long now = System.currentTimeMillis();

            try {
                if (supervisoree.status.firstSeen == null) {
                    logger.debug("first time seeing {}", lifecycleAware);
                    supervisoree.status.firstSeen = now;
                }

                supervisoree.status.lastSeen = now;
                synchronized (lifecycleAware) {
                    if (supervisoree.status.discard) {
                        // Unsupervise has already been called on this.
                        logger.info("Component has already been stopped {}", lifecycleAware);
                        return;
                    } else if (supervisoree.status.error) {
                        logger.info("Component {} is in error state, and Flume will not"
                                + "attempt to change its state", lifecycleAware);
                        return;
                    }

                    supervisoree.status.lastSeenState = lifecycleAware.getLifecycleState();

                    if (!lifecycleAware.getLifecycleState().equals(// 判断守护进程的状态
                            supervisoree.status.desiredState)) {

                        logger.debug("Want to transition {} from {} to {} (failures:{})",
                                new Object[]{lifecycleAware, supervisoree.status.lastSeenState,
                                        supervisoree.status.desiredState,
                                        supervisoree.status.failures});

                        switch (supervisoree.status.desiredState) {
                            case START:
                                try {
                                    // MonitorRunnable的run函数中lifecycleAware.start()说明执行了传入组件的start()方法。
                                    lifecycleAware.start();
                                } catch (Throwable e) {
                                    logger.error("Unable to start " + lifecycleAware
                                            + " - Exception follows.", e);
                                    if (e instanceof Error) {
                                        // This component can never recover, shut it down.
                                        supervisoree.status.desiredState = LifecycleState.STOP;
                                        try {
                                            lifecycleAware.stop();
                                            logger.warn("Component {} stopped, since it could not be"
                                                            + "successfully started due to missing dependencies",
                                                    lifecycleAware);
                                        } catch (Throwable e1) {
                                            logger.error("Unsuccessful attempt to "
                                                    + "shutdown component: {} due to missing dependencies."
                                                    + " Please shutdown the agent"
                                                    + "or disable this component, or the agent will be"
                                                    + "in an undefined state.", e1);
                                            supervisoree.status.error = true;
                                            if (e1 instanceof Error) {
                                                throw (Error) e1;
                                            }
                                            // Set the state to stop, so that the conf poller can
                                            // proceed.
                                        }
                                    }
                                    supervisoree.status.failures++;
                                }
                                break;
                            case STOP:
                                try {
                                    lifecycleAware.stop();
                                } catch (Throwable e) {
                                    logger.error("Unable to stop " + lifecycleAware
                                            + " - Exception follows.", e);
                                    if (e instanceof Error) {
                                        throw (Error) e;
                                    }
                                    supervisoree.status.failures++;
                                }
                                break;
                            default:
                                logger.warn("I refuse to acknowledge {} as a desired state",
                                        supervisoree.status.desiredState);
                        }

                        if (!supervisoree.policy.isValid(lifecycleAware, supervisoree.status)) {
                            logger.error("Policy {} of {} has been violated - supervisor should exit!",
                                    supervisoree.policy, lifecycleAware);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("Unexpected error", t);
            }
            logger.debug("Status check complete");
        }
    }

    private class Purger implements Runnable {

        @Override
        public void run() {
            if (needToPurge) {
                monitorService.purge();
                needToPurge = false;
            }
        }
    }

    public static class Status {  // 守护进程状态
        public Long firstSeen;
        public Long lastSeen;
        public LifecycleState lastSeenState;
        public LifecycleState desiredState;
        public int failures;
        public boolean discard;
        public volatile boolean error;

        @Override
        public String toString() {
            return "{ lastSeen:" + lastSeen + " lastSeenState:" + lastSeenState
                    + " desiredState:" + desiredState + " firstSeen:" + firstSeen
                    + " failures:" + failures + " discard:" + discard + " error:" +
                    error + " }";
        }

    }

    public abstract static class SupervisorPolicy {  // 守护进程 策略

        abstract boolean isValid(LifecycleAware object, Status status);

        public static class AlwaysRestartPolicy extends SupervisorPolicy {

            @Override
            boolean isValid(LifecycleAware object, Status status) {
                return true;
            }
        }

        public static class OnceOnlyPolicy extends SupervisorPolicy {

            @Override
            boolean isValid(LifecycleAware object, Status status) {
                return status.failures == 0;
            }
        }

    }

    private static class Supervisoree {// 守护进程

        public SupervisorPolicy policy; // 主进程 策略
        public Status status;  // 守护进程   状态

        @Override
        public String toString() {
            return "{ status:" + status + " policy:" + policy + " }";
        }

    }

}
