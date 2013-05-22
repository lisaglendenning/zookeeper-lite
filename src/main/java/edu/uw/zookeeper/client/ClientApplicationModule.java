package edu.uw.zookeeper.client;


import java.util.AbstractMap;
import java.util.Map;

import com.typesafe.config.Config;
import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.EnsembleQuorumView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerQuorumView;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.client.ClientModule;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.ConfigurableTime;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceApplication;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.TimeValue;

public enum ClientApplicationModule implements ParameterizedFactory<AbstractMain, Application> {
    INSTANCE;
    
    public static ClientApplicationModule getInstance() {
        return INSTANCE;
    }

    public static class ConfigurableEnsembleViewFactory implements DefaultsFactory<Configuration, EnsembleQuorumView<?>> {

        public static ConfigurableEnsembleViewFactory newInstance() {
            return new ConfigurableEnsembleViewFactory("");
        }
        
        public static final String ARG = "ensemble";
        public static final String CONFIG_KEY = "Ensemble";

        public static final String DEFAULT_ADDRESS = "localhost";
        public static final int DEFAULT_PORT = 2181;
        
        private final String configPath;
        
        protected ConfigurableEnsembleViewFactory(String configPath) {
            this.configPath = configPath;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public EnsembleQuorumView<?> get() {
            return EnsembleQuorumView.ofQuorum(
                    ServerQuorumView.of(ServerInetAddressView.of(
                    DEFAULT_ADDRESS, DEFAULT_PORT)));
        }

        @Override
        public EnsembleQuorumView<?> get(Configuration value) {
            Arguments arguments = value.asArguments();
            if (! arguments.has(ARG)) {
                arguments.add(arguments.newOption(ARG, "Ensemble"));
            }
            arguments.parse();
            Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(ARG, CONFIG_KEY);
            @SuppressWarnings("unchecked")
            Config config = value.withArguments(configPath, args);
            if (config.hasPath(CONFIG_KEY)) {
                String input = config.getString(CONFIG_KEY);
                return EnsembleQuorumView.fromStringQuorum(input);
            } else {
                return get();
            }
        }
    }
    
    public static class TimeoutFactory implements DefaultsFactory<Configuration, TimeValue> {

        public static TimeoutFactory newInstance() {
            return new TimeoutFactory("");
        }

        public static final String CONFIG_PATH = "Client.Timeout";
        public static final long DEFAULT_TIMEOUT_VALUE = 30;
        public static final String DEFAULT_TIMEOUT_UNIT = "SECONDS";

        protected final String configPath;
        protected final ConfigurableTime timeOut;

        protected TimeoutFactory(String configPath) {
            this.configPath = configPath;
            this.timeOut = ConfigurableTime.create(
                    DEFAULT_TIMEOUT_VALUE,
                    DEFAULT_TIMEOUT_UNIT);
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
            if (config.hasPath(CONFIG_PATH)) {
                return timeOut.get(config.getConfig(CONFIG_PATH));
            } else {
                return get();
            }
        }
    }

    @Override
    public Application get(AbstractMain main) {
        ServiceMonitor monitor = main.serviceMonitor();
        AbstractMain.MonitorServiceFactory monitorsFactory = AbstractMain.monitors(monitor);

        Factory<? extends ClientConnectionFactory> clientConnectionFactory = ClientModule.getInstance().get(main);
        ClientConnectionFactory clientConnections = monitorsFactory.apply(clientConnectionFactory.get());

        EnsembleQuorumView<?> ensemble = ConfigurableEnsembleViewFactory.newInstance().get(main.configuration());
        TimeValue timeOut = TimeoutFactory.newInstance().get(main.configuration());
        ParameterizedFactory<Connection, PingingClientCodecConnection> codecFactory = PingingClientCodecConnection.factory(
                main.publisherFactory(), timeOut, main.executors().asScheduledExecutorServiceFactory().get());
        AssignXidProcessor xids = AssignXidProcessor.newInstance();
        EnsembleViewFactory ensembleFactory = EnsembleViewFactory.newInstance(clientConnections, main.publisherFactory(), codecFactory, xids, ensemble, timeOut);
        monitorsFactory.apply(
                ClientProtocolConnectionService.newInstance(ensembleFactory));

        return ServiceApplication.newInstance(main.serviceMonitor());
    }
}