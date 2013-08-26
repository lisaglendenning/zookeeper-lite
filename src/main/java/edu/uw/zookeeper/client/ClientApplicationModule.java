package edu.uw.zookeeper.client;


import java.util.AbstractMap;
import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.Config;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.TimeoutFactory;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Arguments;
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
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Operation.Request;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.protocol.client.PingingClient;

public class ClientApplicationModule implements ParameterizedFactory<RuntimeModule, Application> {

    public static ClientApplicationModule getInstance() {
        return new ClientApplicationModule();
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

    @Override
    public Application get(RuntimeModule runtime) {
        return ServiceApplication.newInstance(createServices(runtime));
    }
    
    protected NetClientModule getNetClientModule(RuntimeModule runtime) {
        return NettyClientModule.newInstance(runtime);
    }
    
    protected ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getCodecFactory() {
        return AssignXidCodec.factory();
    }
    
    protected ClientConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getClientConnectionFactory(RuntimeModule runtime) {
        NetClientModule clientModule = getNetClientModule(runtime);
        TimeValue timeOut = TimeoutFactory.newInstance().get(runtime.configuration());
        ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory = getCodecFactory();
        ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> pingingFactory = 
                PingingClient.factory(timeOut, runtime.executors().asScheduledExecutorServiceFactory().get());
        ClientConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                clientModule.getClientConnectionFactory(
                            codecFactory, pingingFactory).get();
        runtime.serviceMonitor().add(clientConnections);
        return clientConnections;
    }
    
    protected ClientConnectionExecutorService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getClientConnectionExecutorService(RuntimeModule runtime) {
        ClientConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = getClientConnectionFactory(runtime);
        TimeValue timeOut = TimeoutFactory.newInstance().get(runtime.configuration());
        EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleViewFactory.newInstance().get(runtime.configuration());
        final EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<Session, ServerInetAddressView, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>> ensembleFactory = 
                EnsembleViewFactory.newInstance(
                    clientConnections, 
                    ServerInetAddressView.class, 
                    ensemble, 
                    timeOut,
                    runtime.executors().asScheduledExecutorServiceFactory().get());
        ClientConnectionExecutorService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> executor =
                ClientConnectionExecutorService.newInstance(
                        new Factory<ListenableFuture<ClientConnectionExecutor<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>>>() {
                            @Override
                            public ListenableFuture<ClientConnectionExecutor<PingingClient<Request, AssignXidCodec, Connection<Request>>>> get() {
                                return ensembleFactory.get().get();
                            }
                        });
        runtime.serviceMonitor().add(executor);
        return executor;
    }
    
    protected ServiceMonitor createServices(RuntimeModule runtime) {
        getClientConnectionExecutorService(runtime);
        return runtime.serviceMonitor();
    }
}
