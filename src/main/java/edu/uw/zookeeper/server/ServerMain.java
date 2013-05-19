package edu.uw.zookeeper.server;


import java.net.SocketAddress;
import java.util.AbstractMap;
import java.util.Map;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.ServerAddressView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.Singleton;

public abstract class ServerMain extends AbstractMain {

    public static class ConfigurableServerAddressViewFactory implements DefaultsFactory<Configuration, ServerView.Address<?>> {

        public static ConfigurableServerAddressViewFactory newInstance() {
            return newInstance("");
        }

        public static ConfigurableServerAddressViewFactory newInstance(String configPath) {
            return new ConfigurableServerAddressViewFactory(configPath);
        }
        
        public static final String ARG = "server";
        public static final String CONFIG_KEY = "Server";
        public static final String DEFAULT_ADDRESS = "";
        public static final int DEFAULT_PORT = 2181;

        private final String configPath;
        
        protected ConfigurableServerAddressViewFactory(String configPath) {
            this.configPath = configPath;
        }
        
        @Override
        public ServerInetAddressView get() {
            return ServerInetAddressView.of(
                    DEFAULT_ADDRESS, DEFAULT_PORT);
        }

        @Override
        public ServerView.Address<?> get(Configuration value) {
            Arguments arguments = value.asArguments();
            if (! arguments.has(ARG)) {
                arguments.add(arguments.newOption(ARG, "Address"));
            }
            arguments.parse();
            Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(ARG, CONFIG_KEY);
            @SuppressWarnings("unchecked")
            Config config = value.withArguments(configPath, args);
            if (config.hasPath(CONFIG_KEY)) {
            String input = config.getString(CONFIG_KEY);
                try {
                    return ServerAddressView.fromString(input);
                } catch (ClassNotFoundException e) {
                    throw Throwables.propagate(e);
                }
            } else {
                return get();
            }
        }
    }
    
    protected final Singleton<Application> application;
    
    protected ServerMain(Configuration configuration) {
        super(configuration);
        this.application = Factories.lazyFrom(new Factory<Application>() {
            @Override
            public Application get() {
                ServiceMonitor monitor = serviceMonitor();
                MonitorServiceFactory monitorsFactory = monitors(monitor);

                ServerView.Address<?> address = ConfigurableServerAddressViewFactory.newInstance().get(configuration());
                ServerConnectionFactory serverConnections = monitorsFactory.apply(serverConnectionFactory().get(address.get()));
                
                SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(configuration());
                ExpiringSessionManager sessions = ExpiringSessionManager.newInstance(publisherFactory.get(), policy);
                ExpireSessionsTask expires = monitorsFactory.apply(ExpireSessionsTask.newInstance(sessions, executors.asScheduledExecutorServiceFactory().get(), configuration()));

                final ServerExecutor serverExecutor = ServerExecutor.newInstance(executors.asListeningExecutorServiceFactory().get(), publisherFactory(), sessions);
                final Server server = Server.newInstance(publisherFactory(), serverConnections, serverExecutor);
                
                return ServerMain.super.application();
            }
        });
    }

    @Override
    protected Application application() {
        return application.get();
    }
    
    protected abstract ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory> serverConnectionFactory();
}
