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

package org.apache.rocketmq.proxy;

import com.google.common.collect.Lists;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.plain.PlainAccessValidator;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.GrpcServer;
import org.apache.rocketmq.proxy.grpc.GrpcServerBuilder;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.metrics.ProxyMetricsManager;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.RemotingProtocolServer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;

public class ProxyStartup {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final ProxyStartAndShutdown PROXY_START_AND_SHUTDOWN = new ProxyStartAndShutdown();

    private static class ProxyStartAndShutdown extends AbstractStartAndShutdown {
        @Override
        public void appendStartAndShutdown(StartAndShutdown startAndShutdown) {
            super.appendStartAndShutdown(startAndShutdown);
        }
    }

    /**
     * 应用程序的主入口点。
     * 初始化并启动代理服务器。
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        try {
            // 从命令行解析参数
            CommandLineArgument commandLineArgument = parseCommandLineArgument(args);
            // 初始化配置
            initConfiguration(commandLineArgument);

            // 初始化代理的线程池监控
            initThreadPoolMonitor();

            // 创建处理请求的线程池
            ThreadPoolExecutor executor = createServerExecutor();

            // 创建消息处理实例
            MessagingProcessor messagingProcessor = createMessagingProcessor();

            // 加载访问验证规则
            List<AccessValidator> accessValidators = loadAccessValidators();
            // 创建 gRPC 服务器
            GrpcServer grpcServer = GrpcServerBuilder.newBuilder(executor, ConfigurationManager.getProxyConfig().getGrpcServerPort())
                .addService(createServiceProcessor(messagingProcessor))
                .addService(ChannelzService.newInstance(100))
                .addService(ProtoReflectionService.newInstance())
                .configInterceptor(accessValidators)
                .shutdownTime(ConfigurationManager.getProxyConfig().getGrpcShutdownTimeSeconds(), TimeUnit.SECONDS)
                .build();
            // 注册 gRPC 服务器启动与关闭钩子
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(grpcServer);

            // 创建并初始化远程协议服务器
            RemotingProtocolServer remotingServer = new RemotingProtocolServer(messagingProcessor, accessValidators);
            // 注册远程服务器启动与关闭钩子
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(remotingServer);

            // 依次启动服务器
            PROXY_START_AND_SHUTDOWN.start();

            // 添加 JVM 关闭钩子以优雅地关闭服务器
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("尝试关闭服务器");
                try {
                    PROXY_START_AND_SHUTDOWN.preShutdown();
                    PROXY_START_AND_SHUTDOWN.shutdown();
                } catch (Exception e) {
                    log.error("关闭 rocketmq-proxy 时出现错误", e);
                }
            }));
        } catch (Exception e) {
            e.printStackTrace();
            log.error("发现未预期的错误", e);
            // 退出应用，并设置错误状态码
            System.exit(1);
        }

        // 打印并记录启动成功的消息
        System.out.printf("%s%n", new Date() + " rocketmq-proxy 启动成功");
        log.info(new Date() + " rocketmq-proxy 启动成功");
    }


    protected static List<AccessValidator> loadAccessValidators() {
        List<AccessValidator> accessValidators = ServiceProvider.load(AccessValidator.class);
        if (accessValidators.isEmpty()) {
            log.info("ServiceProvider loaded no AccessValidator, using default org.apache.rocketmq.acl.plain.PlainAccessValidator");
            accessValidators.add(new PlainAccessValidator());
        }
        return accessValidators;
    }

    protected static void initConfiguration(CommandLineArgument commandLineArgument) throws Exception {
        if (StringUtils.isNotBlank(commandLineArgument.getProxyConfigPath())) {
            System.setProperty(Configuration.CONFIG_PATH_PROPERTY, commandLineArgument.getProxyConfigPath());
        }
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
        setConfigFromCommandLineArgument(commandLineArgument);
        log.info("Current configuration: " + ConfigurationManager.formatProxyConfig());

    }

    protected static CommandLineArgument parseCommandLineArgument(String[] args) {
        CommandLine commandLine = ServerUtil.parseCmdLine("mqproxy", args,
            buildCommandlineOptions(), new DefaultParser());
        if (commandLine == null) {
            throw new RuntimeException("parse command line argument failed");
        }

        CommandLineArgument commandLineArgument = new CommandLineArgument();
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), commandLineArgument);
        return commandLineArgument;
    }

    private static Options buildCommandlineOptions() {
        Options options = ServerUtil.buildCommandlineOptions(new Options());

        Option opt = new Option("bc", "brokerConfigPath", true, "Broker config file path for local mode");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("pc", "proxyConfigPath", true, "Proxy config file path");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("pm", "proxyMode", true, "Proxy run in local or cluster mode");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private static void setConfigFromCommandLineArgument(CommandLineArgument commandLineArgument) {
        if (StringUtils.isNotBlank(commandLineArgument.getNamesrvAddr())) {
            ConfigurationManager.getProxyConfig().setNamesrvAddr(commandLineArgument.getNamesrvAddr());
        }
        if (StringUtils.isNotBlank(commandLineArgument.getBrokerConfigPath())) {
            ConfigurationManager.getProxyConfig().setBrokerConfigPath(commandLineArgument.getBrokerConfigPath());
        }
        if (StringUtils.isNotBlank(commandLineArgument.getProxyMode())) {
            ConfigurationManager.getProxyConfig().setProxyMode(commandLineArgument.getProxyMode());
        }
    }

    protected static MessagingProcessor createMessagingProcessor() {
        String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();
        MessagingProcessor messagingProcessor;

        if (ProxyMode.isClusterMode(proxyModeStr)) {
            messagingProcessor = DefaultMessagingProcessor.createForClusterMode();
            ProxyMetricsManager proxyMetricsManager = ProxyMetricsManager.initClusterMode(ConfigurationManager.getProxyConfig());
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(proxyMetricsManager);
        } else if (ProxyMode.isLocalMode(proxyModeStr)) {
            BrokerController brokerController = createBrokerController();
            ProxyMetricsManager.initLocalMode(brokerController.getBrokerMetricsManager(), ConfigurationManager.getProxyConfig());
            StartAndShutdown brokerControllerWrapper = new StartAndShutdown() {
                @Override
                public void start() throws Exception {
                    brokerController.start();
                    String tip = "The broker[" + brokerController.getBrokerConfig().getBrokerName() + ", "
                        + brokerController.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
                    if (null != brokerController.getBrokerConfig().getNamesrvAddr()) {
                        tip += " and name server is " + brokerController.getBrokerConfig().getNamesrvAddr();
                    }
                    log.info(tip);
                }

                @Override
                public void shutdown() throws Exception {
                    brokerController.shutdown();
                }
            };
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(brokerControllerWrapper);
            messagingProcessor = DefaultMessagingProcessor.createForLocalMode(brokerController);
        } else {
            throw new IllegalArgumentException("try to start grpc server with wrong mode, use 'local' or 'cluster'");
        }
        PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(messagingProcessor);
        return messagingProcessor;
    }

    private static GrpcMessagingApplication createServiceProcessor(MessagingProcessor messagingProcessor) {
        GrpcMessagingApplication application = GrpcMessagingApplication.create(messagingProcessor);
        PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(application);
        return application;
    }

    protected static BrokerController createBrokerController() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        List<String> brokerStartupArgList = Lists.newArrayList("-c", config.getBrokerConfigPath());
        if (StringUtils.isNotBlank(config.getNamesrvAddr())) {
            brokerStartupArgList.add("-n");
            brokerStartupArgList.add(config.getNamesrvAddr());
        }
        String[] brokerStartupArgs = brokerStartupArgList.toArray(new String[0]);
        return BrokerStartup.createBrokerController(brokerStartupArgs);
    }

    /**
     * 创建一个用于服务器的线程池执行器
     * 该方法根据配置文件获取线程池的数量和队列容量，并使用这些参数创建一个固定大小的线程池
     * 线程池的拒绝策略为阻塞，确保在队列满时不会丢弃任务
     * 线程池的监控实例会记录线程的状态，便于问题排查
     * 在应用程序关闭时，线程池将被正确关闭，以确保资源的正确释放
     *
     * @return ThreadPoolExecutor 返回创建的线程池执行器
     */
    public static ThreadPoolExecutor createServerExecutor() {
        // 获取代理配置对象
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        // 从配置对象中获取线程池数量和队列容量
        int threadPoolNums = config.getGrpcThreadPoolNums();
        int threadPoolQueueCapacity = config.getGrpcThreadPoolQueueCapacity();
        // 使用获取的参数创建并监控线程池
        ThreadPoolExecutor executor = ThreadPoolMonitor.createAndMonitor(
            threadPoolNums,
            threadPoolNums,
            1, TimeUnit.MINUTES,
            "GrpcRequestExecutorThread",
            threadPoolQueueCapacity
        );
        // 注册线程池的关闭操作，确保在应用程序关闭时正确关闭线程池
        PROXY_START_AND_SHUTDOWN.appendShutdown(executor::shutdown);
        // 返回创建的线程池执行器
        return executor;
    }

    /**
     * 初始化线程池监控器
     * 该方法通过配置管理器获取代理配置，并使用这些配置来初始化线程池监控器
     * 监控器主要用于监控线程池的状态，并根据配置决定是否打印JStack以及打印频率
     */
    public static void initThreadPoolMonitor() {
        // 获取代理配置
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        // 使用配置初始化线程池监控器
        ThreadPoolMonitor.config(
            LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME),
            LoggerFactory.getLogger(LoggerName.PROXY_WATER_MARK_LOGGER_NAME),
            config.isEnablePrintJstack(), config.getPrintJstackInMillis(),
            config.getPrintThreadPoolStatusInMillis());
        // 初始化线程池监控器
        ThreadPoolMonitor.init();
    }
}
