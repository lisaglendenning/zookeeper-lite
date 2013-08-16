package edu.uw.zookeeper.client;


import java.util.AbstractMap;
import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.Config;
import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Arguments;
import edu.uw.zookeeper.common.ConfigurableTime;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Operation.Request;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.AssignXidProcessor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.protocol.client.ClientProtocolCodec;
import edu.uw.zookeeper.protocol.client.PingingClient;

public enum ClientApplicationModule implements ParameterizedFactory<RuntimeModule, Application> {
    INSTANCE;
    
    public static ClientApplicationModule getInstance() {
        return INSTANCE;
    }

    public static class ConfigurableEnsembleViewFactory implements DefaultsFactory<Configuration, EnsembleView<ServerInetAddressView>> {

        public static ConfigurableEnsembleViewFactory newInstance() {
            return newInstance(DEFAULT_CONFIG_PATH);
        }
        
        public static ConfigurableEnsembleViewFactory newInstance(String configPath) {
            return newInstance(
                    configPath, DEFAULT_CONFIG_KEY, DEFAULT_ARG, DEFAULT_ADDRESS, DEFAULT_PORT);
        }

        public static ConfigurableEnsembleViewFactory newInstance(
                String configPath, 
                String configKey, 
                String arg,
                String defaultAddress, 
                int defaultPort) {
            return new ConfigurableEnsembleViewFactory(
                    configPath, configKey, arg, defaultAddress, defaultPort);
        }

        public static final String DEFAULT_CONFIG_PATH = "";
        public static final String DEFAULT_ARG = "ensemble";
        public static final String DEFAULT_CONFIG_KEY = "Ensemble";
        public static final String DEFAULT_ADDRESS = "localhost";
        public static final int DEFAULT_PORT = 2181;
        
        private final String configPath;
        private final String configKey;
        private final String arg;
        private final String defaultAddress;
        private final int defaultPort;
        
        public ConfigurableEnsembleViewFactory(
                String configPath, 
                String configKey, 
                String arg, 
                String defaultAddress, 
                int defaultPort) {
            this.configPath = configPath;
            this.arg = arg;
            this.configKey = configKey;
            this.defaultAddress = defaultAddress;
            this.defaultPort = defaultPort;
        }
        
        @Override
        public EnsembleView<ServerInetAddressView> get() {
            return EnsembleView.of(ServerInetAddressView.of(
                    defaultAddress, defaultPort));
        }

        @Override
        public EnsembleView<ServerInetAddressView> get(Configuration value) {
            Arguments arguments = value.asArguments();
            if (! arguments.has(arg)) {
                arguments.add(arguments.newOption(arg, "Ensemble"));
            }
            arguments.parse();
            Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(arg, configKey);
            @SuppressWarnings("unchecked")
            Config config = value.withArguments(configPath, args);
            if (config.hasPath(configKey)) {
                String input = config.getString(configKey);
                return EnsembleView.fromString(input);
            } else {
                return get();
            }
        }
    }
    
    public static class TimeoutFactory implements DefaultsFactory<Configuration, TimeValue> {

        public static TimeoutFactory newInstance() {
            return newInstance(DEFAULT_CONFIG_PATH);
        }

        public static TimeoutFactory newInstance(String configPath) {
            return newInstance(configPath, DEFAULT_CONFIG_KEY, DEFAULT_TIMEOUT_VALUE, DEFAULT_TIMEOUT_UNIT);
        }

        public static TimeoutFactory newInstance(
                String configPath,
                String configKey,
                long defaultTimeOutValue,
                String defaultTimeOutUnit) {
            return new TimeoutFactory(configPath, configKey, defaultTimeOutValue, defaultTimeOutUnit);
        }

        public static final String DEFAULT_CONFIG_PATH = "";
        public static final String DEFAULT_CONFIG_KEY = "Timeout";
        public static final long DEFAULT_TIMEOUT_VALUE = 30;
        public static final String DEFAULT_TIMEOUT_UNIT = "SECONDS";

        protected final String configPath;
        protected final String configKey;
        protected final ConfigurableTime timeOut;

        protected TimeoutFactory(
                String configPath,
                String configKey,
                long defaultTimeOutValue,
                String defaultTimeOutUnit) {
            this.configPath = configPath;
            this.configKey = configKey;
            this.timeOut = ConfigurableTime.create(
                    defaultTimeOutValue,
                    defaultTimeOutUnit);
        }

        @Override
        public TimeValue get() {
            return timeOut.get();
        }

        @Override
        public TimeValue get(Configuration value) {
            Config config = value.asConfig();
            if (configPath.length() > 0 && config.hasPath(configPath)) {
                config = config.getConfig(configPath);
            }
            if (config.hasPath(configKey)) {
                return timeOut.get(config.getConfig(configKey));
            } else {
                return get();
            }
        }
    }
    
    public static ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory() {
        return new ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>>() {
            @Override
            public Pair<Class<Operation.Request>, AssignXidCodec> get(
                    Publisher value) {
                return Pair.create(Operation.Request.class, AssignXidCodec.newInstance(
                        AssignXidProcessor.newInstance(),
                        ClientProtocolCodec.newInstance(value)));
            }
        };
    }

    @Override
    public Application get(RuntimeModule runtime) {
        ServiceMonitor monitor = runtime.serviceMonitor();
        AbstractMain.MonitorServiceFactory monitorsFactory = AbstractMain.monitors(monitor);

        NettyClientModule clientModule = NettyClientModule.newInstance(runtime);

        TimeValue timeOut = TimeoutFactory.newInstance().get(runtime.configuration());
        ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory = codecFactory();
        ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> pingingFactory = 
                PingingClient.factory(timeOut, runtime.executors().asScheduledExecutorServiceFactory().get());
        ClientConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                monitorsFactory.apply(
                    clientModule.getClientConnectionFactory(
                            codecFactory, pingingFactory).get());

        EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleViewFactory.newInstance().get(runtime.configuration());
        final EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<Session, ServerInetAddressView, PingingClient<Operation.Request, AssignXidCodec, Connection<Operation.Request>>>> ensembleFactory = 
                EnsembleViewFactory.newInstance(
                    clientConnections, 
                    ServerInetAddressView.class, 
                    ensemble, 
                    timeOut);
        monitorsFactory.apply(
                ClientConnectionExecutorService.newInstance(
                        new Factory<ListenableFuture<ClientConnectionExecutor<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>>>() {
                            @Override
                            public ListenableFuture<ClientConnectionExecutor<PingingClient<Request, AssignXidCodec, Connection<Request>>>> get() {
                                return ensembleFactory.get().get();
                            }
                        }));

        return ServiceApplication.newInstance(runtime.serviceMonitor());
    }
}
