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

package org.apache.flume.node;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.commons.cli.*;
import org.apache.flume.*;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.MonitoringType;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * 源码分析： https://www.jianshu.com/p/0c13fc947155
 * 下面我们总结一下整个flume的调用顺序。
 * Application -->LifecycleSupervisor-(3s调度一次)--> MonitorRunnable --->
 * PollingPropertiesFileConfigurationProvider-(30s调度一次)--> FileWatcherRunnable--->EventBus--->Application
 */
public class Application {

    private static final Logger logger = LoggerFactory
            .getLogger(Application.class);

    public static final String CONF_MONITOR_CLASS = "flume.monitoring.type";
    public static final String CONF_MONITOR_PREFIX = "flume.monitoring.";

    private final List<LifecycleAware> components;
    private final LifecycleSupervisor supervisor;
    private MaterializedConfiguration materializedConfiguration;
    private MonitorService monitorServer;

    public Application() {
        this(new ArrayList<LifecycleAware>(0));
    }

    public Application(List<LifecycleAware> components) {
        this.components = components;
        supervisor = new LifecycleSupervisor();
    }

    public synchronized void start() {
        // 对所有的组件进行启动
        for (LifecycleAware component : components) {
            // 具体每个component是怎么启动的，我们可以深入到LifecycleSupervisor.supervise()函数中查看
            supervisor.supervise(component, new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
        }
    }

    /**
     * 用到了： import com.google.common.eventbus.Subscribe;
     * 在类Application中，我们可以找到事件处理方法handleConfigurationEvent(MaterializedConfiguration conf)。
     * <p>
     * flume的启动方式分为两种：
     * （1）自动监听配置文件变化利用eventBus发送事件重启所有组件；
     * （2）没有监听配置文件直接启动。两种方式的启动入口都在类Application的
     * public void handleConfigurationEvent(MaterializedConfiguration conf) {...}函数中。
     *
     * @param conf
     */
    @Subscribe
    public synchronized void handleConfigurationEvent(MaterializedConfiguration conf) {
        // 该方法调用stopAllComponents()和startAllComponents(conf)函数对所有的组件进行了重启。
        stopAllComponents();
        startAllComponents(conf);
    }

    public synchronized void stop() {
        supervisor.stop();
        if (monitorServer != null) {
            monitorServer.stop();
        }
    }

    private void stopAllComponents() {
        if (this.materializedConfiguration != null) {
            logger.info("Shutting down configuration: {}", this.materializedConfiguration);
            for (Entry<String, SourceRunner> entry :
                    this.materializedConfiguration.getSourceRunners().entrySet()) {
                try {
                    logger.info("Stopping Source " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }

            for (Entry<String, SinkRunner> entry :
                    this.materializedConfiguration.getSinkRunners().entrySet()) {
                try {
                    logger.info("Stopping Sink " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }

            for (Entry<String, Channel> entry :
                    this.materializedConfiguration.getChannels().entrySet()) {
                try {
                    logger.info("Stopping Channel " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }
        }
        if (monitorServer != null) {
            monitorServer.stop();
        }
    }

    /**
     * // MaterializedConfiguration对象作为startAllComponents函数的形参，
     * 在Application里调用了PropertiesFileConfigurationProvider的getConfiguration()方法得到MaterializedConfiguration对象：
     * 首先启动channel，确保channel都启动之后再启动sink和source，以channel为例我们来看下flume是怎么启动他们的：
     * 启动顺序是：Channel -> 等待所有channel都起来 -> Sink -> Source
     * 关闭顺序反之：sources——>sinks——>channels；
     * 在程序启动时，会启动MaterializedConfiguration中的所有的SourceRunner、Channel、SinkRunner。
     * 其中Channel的启动，没做什么特别的事情，就是初始化一下状态、创建一下计数器，算做一个被动的角色。
     *
     * @param materializedConfiguration
     */
    private void startAllComponents(MaterializedConfiguration materializedConfiguration) {
        logger.info("Starting new configuration:{}", materializedConfiguration);

        this.materializedConfiguration = materializedConfiguration;
         //首先启动channel
        for (Entry<String, Channel> entry : materializedConfiguration.getChannels().entrySet()) {
            try {
                logger.info("Starting Channel " + entry.getKey());
                supervisor.supervise(entry.getValue(),
                        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        /*
         * Wait for all channels to start.  等待所有的channel  启动
         */
        for (Channel ch : materializedConfiguration.getChannels().values()) {
            while (ch.getLifecycleState() != LifecycleState.START
                    && !supervisor.isComponentInErrorState(ch)) {
                try {
                    logger.info("Waiting for channel: " + ch.getName() +
                            " to start. Sleeping for 500 ms");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for channel to start.", e);
                    Throwables.propagate(e);
                }
            }
        }
        // 然后启动 sink
        for (Entry<String, SinkRunner> entry : materializedConfiguration.getSinkRunners().entrySet()) {
            try {
                logger.info("Starting Sink " + entry.getKey());
                supervisor.supervise(entry.getValue(),
                        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }
        // 最后启动 source
        for (Entry<String, SourceRunner> entry :
                materializedConfiguration.getSourceRunners().entrySet()) {
            try {
                logger.info("Starting Source " + entry.getKey());
                supervisor.supervise(entry.getValue(),
                        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        this.loadMonitoring();
    }

    @SuppressWarnings("unchecked")
    private void loadMonitoring() {
        Properties systemProps = System.getProperties();
        Set<String> keys = systemProps.stringPropertyNames();
        try {
            if (keys.contains(CONF_MONITOR_CLASS)) {
                String monitorType = systemProps.getProperty(CONF_MONITOR_CLASS);
                Class<? extends MonitorService> klass;
                try {
                    //Is it a known type?
                    klass = MonitoringType.valueOf(
                            monitorType.toUpperCase(Locale.ENGLISH)).getMonitorClass();
                } catch (Exception e) {
                    //Not a known type, use FQCN
                    klass = (Class<? extends MonitorService>) Class.forName(monitorType);
                }
                this.monitorServer = klass.newInstance();
                Context context = new Context();
                for (String key : keys) {
                    if (key.startsWith(CONF_MONITOR_PREFIX)) {
                        context.put(key.substring(CONF_MONITOR_PREFIX.length()),
                                systemProps.getProperty(key));
                    }
                }
                monitorServer.configure(context);
                monitorServer.start();
            }
        } catch (Exception e) {
            logger.warn("Error starting monitoring. "
                    + "Monitoring might not be available.", e);
        }

    }

    public static void main(String[] args) {

        try {

            boolean isZkConfigured = false;

            Options options = new Options();

            Option option = new Option("n", "name", true, "the name of this agent");
            option.setRequired(true);
            options.addOption(option);

            option = new Option("f", "conf-file", true, "specify a config file (required if -z missing)");
            option.setRequired(false);
            options.addOption(option);

            option = new Option(null, "no-reload-conf", false, "do not reload config file if changed");
            options.addOption(option);

            // Options for Zookeeper
            option = new Option("z", "zkConnString", true, "specify the ZooKeeper connection to use (required if -f missing)");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("p", "zkBasePath", true, "specify the base path in ZooKeeper for agent configs");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("h", "help", false, "display help text");
            options.addOption(option);

            CommandLineParser parser = new GnuParser();
            CommandLine commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                new HelpFormatter().printHelp("flume-ng agent", options, true);
                return;
            }

            String agentName = commandLine.getOptionValue('n');
            boolean reload = !commandLine.hasOption("no-reload-conf");//如果含有参数no-reload-conf，则 reload=true

            if (commandLine.hasOption('z') || commandLine.hasOption("zkConnString")) {
                isZkConfigured = true;
            }
            Application application = null;
            if (isZkConfigured) { // 如果设置了zk的配置
                // get options
                String zkConnectionStr = commandLine.getOptionValue('z');
                String baseZkPath = commandLine.getOptionValue('p');

                if (reload) {
                    EventBus eventBus = new EventBus(agentName + "-event-bus");
                    List<LifecycleAware> components = Lists.newArrayList();
                    PollingZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                            new PollingZooKeeperConfigurationProvider(agentName, zkConnectionStr, baseZkPath, eventBus);
                    components.add(zookeeperConfigurationProvider);
                    application = new Application(components);
                    eventBus.register(application);
                    /**
                     * 以上代码用到了guava EventBus，guava的EventBus是观察者模式的一种优雅的解决方案，
                     *  利用EventBus实现事件的发布和订阅，可以节省很多工作量。
                     *  guava EventBus的原理和使用参见：https://www.jianshu.com/p/f8ba312904f4 。
                     *  EventBus的观察者（事件订阅者）需要用@Subscribe 注释标注的函数来处理事件发布者发过来的事件。
                     *  EventBus.register()用来注册观察者。
                     * 链接：https://www.jianshu.com/p/8a19186c65d3
                     */
                } else {
                    StaticZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                            new StaticZooKeeperConfigurationProvider(agentName, zkConnectionStr, baseZkPath);
                    application = new Application();
                    application.handleConfigurationEvent(zookeeperConfigurationProvider.getConfiguration());
                }
            } else { // 没有zk的配置
                File configurationFile = new File(commandLine.getOptionValue('f'));

                /*
                 * The following is to ensure that by default the agent will fail on
                 * startup if the file does not exist.
                 */
                if (!configurationFile.exists()) {
                    // If command line invocation, then need to fail fast
                    if (System.getProperty(Constants.SYSPROP_CALLED_FROM_SERVICE) ==
                            null) {
                        String path = configurationFile.getPath();
                        try {
                            path = configurationFile.getCanonicalPath();
                        } catch (IOException ex) {
                            logger.error("Failed to read canonical path for file: " + path,
                                    ex);
                        }
                        throw new ParseException(
                                "The specified configuration file does not exist: " + path);
                    }
                }
                List<LifecycleAware> components = Lists.newArrayList();

                if (reload) {
                    EventBus eventBus = new EventBus(agentName + "-event-bus");
                    // PollingPropertiesFileConfigurationProvider 该类实现了接口LifecycleAware，flume中所有的组件都实现自该接口。
                    PollingPropertiesFileConfigurationProvider configurationProvider =
                            new PollingPropertiesFileConfigurationProvider(
                                    agentName, configurationFile, eventBus, 30);
                    components.add(configurationProvider);
                    //最终，PollingPropertiesFileConfigurationProvider的对象被添加到全局属性List<LifecycleAware> components中
                    application = new Application(components);
                    eventBus.register(application);
                    /**
                     * 以上代码用到了guava EventBus，guava的EventBus是观察者模式的一种优雅的解决方案，
                     *  利用EventBus实现事件的发布和订阅，可以节省很多工作量。
                     *  guava EventBus的原理和使用参见：https://www.jianshu.com/p/f8ba312904f4 。
                     *  EventBus的观察者（事件订阅者）需要用@Subscribe 注释标注的函数来处理事件发布者发过来的事件。
                     *  EventBus.register()用来注册观察者。
                     * 链接：https://www.jianshu.com/p/8a19186c65d3
                     */
                } else {
                    PropertiesFileConfigurationProvider configurationProvider =
                            new PropertiesFileConfigurationProvider(agentName, configurationFile);
                    application = new Application();
                    application.handleConfigurationEvent(configurationProvider.getConfiguration());
                }
            }
            //然后调用Application的start()方法，对components进行启动
            application.start();

            final Application appReference = application;
            Runtime.getRuntime().addShutdownHook(new Thread("agent-shutdown-hook") {
                @Override
                public void run() {
                    appReference.stop();
                }
            });

        } catch (Exception e) {
            logger.error("A fatal error occurred while running. Exception follows.", e);
        }
    }
}