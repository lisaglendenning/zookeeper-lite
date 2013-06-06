package edu.uw.zookeeper.server;

import java.net.SocketAddress;
import java.util.AbstractMap;
import java.util.Map;

import com.typesafe.config.Config;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerAddressView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.Connection.CodecFactory;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.netty.server.ServerModule;
import edu.uw.zookeeper.protocol.CodecConnection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceApplication;
import edu.uw.zookeeper.util.ServiceMonitor;

public enum ServerApplicationModule implements ParameterizedFactory<RuntimeModule, Application> {
    INSTANCE;
    
    public static ServerApplicationModule getInstance() {
        return INSTANCE;
    }

    public static class ConfigurableServerAddressViewFactory implements DefaultsFactory<Configuration, ServerInetAddressView> {

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
        public ServerInetAddressView get(Configuration value) {
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
                return ServerInetAddressView.fromString(input);
            } else {
                return get();
            }
        }
    }
    
    @Override
    public Application get(RuntimeModule runtime) {
        ServiceMonitor monitor = runtime.serviceMonitor();
        AbstractMain.MonitorServiceFactory monitorsFactory = AbstractMain.monitors(monitor);

        SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(runtime.configuration());
        ExpiringSessionManager sessions = ExpiringSessionManager.newInstance(runtime.publisherFactory().get(), policy);
        ExpireSessionsTask expires = monitorsFactory.apply(ExpireSessionsTask.newInstance(sessions, runtime.executors().asScheduledExecutorServiceFactory().get(), runtime.configuration()));

        final ServerExecutor serverExecutor = ServerExecutor.newInstance(runtime.executors().asListeningExecutorServiceFactory().get(), runtime.publisherFactory(), sessions);

        ParameterizedFactory<Connection<Message.ServerMessage>, ServerCodecConnection> codecConnectionFactory = 
                ServerCodecConnection.factory(runtime.publisherFactory());
        CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection> codecFactory = CodecConnection.factory(codecConnectionFactory);
        ParameterizedFactory<Connection.CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection>, ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>>> connectionFactory = ServerModule.factory(runtime);
        ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>> serverConnectionFactory = connectionFactory.get(codecFactory);
        ServerView.Address<?> address = ConfigurableServerAddressViewFactory.newInstance().get(runtime.configuration());
        ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> serverConnections = monitorsFactory.apply(serverConnectionFactory.get(address.get()));
        
        final Server server = Server.newInstance(runtime.publisherFactory(), serverConnections, serverExecutor);
        
        return ServiceApplication.newInstance(runtime.serviceMonitor());
    }

}
