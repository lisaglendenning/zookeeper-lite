package edu.uw.zookeeper.client;


import com.typesafe.config.Config;
import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.ConfigurableTime;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.Singleton;
import edu.uw.zookeeper.util.TimeValue;

public abstract class ClientMain extends AbstractMain {

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

    protected final Singleton<Application> application;
    
    protected ClientMain(Configuration configuration) {
        super(configuration);
        this.application = Factories.lazyFrom(new Factory<Application>() {
            @Override
            public Application get() {
                ServiceMonitor monitor = serviceMonitor();
                MonitorServiceFactory monitorsFactory = monitors(monitor);
        
                ClientConnectionFactory clientConnections = monitorsFactory.apply(clientConnectionFactory().get());
        
                TimeValue timeOut = TimeoutFactory.newInstance().get(configuration());
                EnsembleView ensemble = ConfigurableEnsembleViewFactory.newInstance().get(configuration());
                ParameterizedFactory<Connection, PingingClientCodecConnection> codecFactory = PingingClientCodecConnection.factory(
                        publisherFactory(), timeOut, executors().asScheduledExecutorServiceFactory().get());
                final EnsembleFactory ensembleFactory = EnsembleFactory.newInstance(clientConnections, codecFactory, ensemble, timeOut);
                final AssignXidProcessor xids = AssignXidProcessor.newInstance();
                Factory<SessionClient> clientFactory = new Factory<SessionClient>() {
                    @Override
                    public SessionClient get() {
                        return SessionClient.newInstance(xids, ensembleFactory.get());
                    }
                };
                
                monitorsFactory.apply(
                        SessionClientService.newInstance(clientFactory));
        
                return ClientMain.super.application();
            }
        });
    }

    @Override
    protected Application application() {
        return application.get();
    }
    
    protected abstract Factory<? extends ClientConnectionFactory> clientConnectionFactory();
}
