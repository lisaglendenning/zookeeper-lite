package edu.uw.zookeeper.client;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigValueFactory;

import edu.uw.zookeeper.Connection;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Configurable;
import edu.uw.zookeeper.util.ConfigurableSocketAddress;
import edu.uw.zookeeper.util.Configuration;

public class ClientConnectionFactory implements Configurable,
        Provider<Connection> {

    public static ClientConnectionFactory create(
            ClientConnectionGroup connections, Arguments arguments,
            Configuration configuration) throws Exception {
        return new ClientConnectionFactory(connections, arguments,
                configuration);
    }

    public static ClientConnectionFactory create(
            ClientConnectionGroup connections) throws Exception {
        return new ClientConnectionFactory(connections);
    }

    public static final String ARG_ADDRESS = "address";
    public static final String ARG_PORT = "port";

    public static final String CONFIG_PATH = "Server.Address";
    public static final String DEFAULT_ADDRESS = "localhost";
    public static final int DEFAULT_PORT = 2181;

    protected final Logger logger = LoggerFactory
            .getLogger(ClientConnectionFactory.class);
    protected final ClientConnectionGroup connections;
    protected final ConfigurableSocketAddress address;

    @Inject
    protected ClientConnectionFactory(ClientConnectionGroup connections,
            Arguments arguments, Configuration configuration) throws Exception {
        this(connections, configuration);

        arguments.add(arguments.newOption(ARG_ADDRESS, "ServerAddress")).add(
                arguments.newOption(ARG_PORT, "ServerPort"));
        arguments.parse();
        Map<String, Object> args = Maps.newHashMap();
        if (arguments.hasValue(ARG_ADDRESS)) {
            args.put(ConfigurableSocketAddress.Factory.KEY_ADDRESS,
                    arguments.getValue(ARG_ADDRESS));
        }
        if (arguments.hasValue(ARG_PORT)) {
            args.put(ConfigurableSocketAddress.Factory.KEY_PORT,
                    Integer.valueOf(arguments.getValue(ARG_PORT)));
        }
        Config config = ConfigValueFactory.fromMap(args).toConfig();
        if (! config.isEmpty()) {
            try {
                config = config.withFallback(configuration.get().getConfig(CONFIG_PATH));
            } catch (ConfigException.Missing e) {}
            address.get(config);
        }
    }

    protected ClientConnectionFactory(
            ClientConnectionGroup connections,
            Configuration configuration)
            throws Exception {
        this(connections);
        configure(configuration);
    }

    protected ClientConnectionFactory(ClientConnectionGroup connections)
            throws Exception {
        this.connections = connections;
        this.address = ConfigurableSocketAddress.create(
                DEFAULT_PORT, DEFAULT_ADDRESS);
    }

    @Override
    public void configure(Configuration configuration) {
        try {
            address.get(configuration.get().getConfig(CONFIG_PATH));
        } catch (ConfigException.Missing e) {}
    }

    @Override
    public Connection get() {
        Connection connection = null;
        SocketAddress server = address.get();
        logger.debug("Connecting to {}", server);
        try {
            connection = connections.connect(server).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }
}
