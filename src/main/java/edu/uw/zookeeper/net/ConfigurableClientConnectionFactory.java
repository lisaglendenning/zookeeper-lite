package edu.uw.zookeeper.net;

import java.net.SocketAddress;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.ConfigurableSocketAddress;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;

public class ConfigurableClientConnectionFactory
  implements DefaultsFactory<Configuration, Connection> {

    public static ConfigurableClientConnectionFactory create(
            ClientConnectionFactory connections) {
        return create(
                connections,
                ConfigurableSocketAddress.create(DEFAULT_PORT, DEFAULT_ADDRESS));
    }

    public static ConfigurableClientConnectionFactory create(
            ClientConnectionFactory connections,
            DefaultsFactory<Config, SocketAddress> addressFactory) {
        return new ConfigurableClientConnectionFactory(connections, addressFactory);
    }

    public static final String ARG_ADDRESS = "address";
    public static final String ARG_PORT = "port";

    public static final String CONFIG_PATH = "Server.Address";
    public static final String DEFAULT_ADDRESS = "localhost";
    public static final int DEFAULT_PORT = 2181;

    protected final ClientConnectionFactory connections;
    protected final DefaultsFactory<Config, SocketAddress> addressFactory;

    protected ConfigurableClientConnectionFactory(
            ClientConnectionFactory connections,
            DefaultsFactory<Config, SocketAddress> addressFactory) {
        this.connections = connections;
        this.addressFactory = addressFactory;
    }

    @Override
    public Connection get() {
        return connect(addressFactory.get());
    }

    @Override
    public Connection get(Configuration value) {
        Arguments arguments = value.asArguments();
        if (! arguments.has(ARG_ADDRESS)) {
            arguments.add(arguments.newOption(ARG_ADDRESS, "ServerAddress"));
        }
        if (! arguments.has(ARG_PORT)) {
            arguments.add(arguments.newOption(ARG_PORT, "ServerPort"));
        }
        arguments.parse();

        Map<String, Object> args = Maps.newHashMap();
        if (arguments.hasValue(ARG_ADDRESS)) {
            args.put(ConfigurableSocketAddress.KEY_ADDRESS,
                    arguments.getValue(ARG_ADDRESS));
        }
        if (arguments.hasValue(ARG_PORT)) {
            args.put(ConfigurableSocketAddress.KEY_PORT,
                    Integer.valueOf(arguments.getValue(ARG_PORT)));
        }
        Config config = value.asConfig().hasPath(CONFIG_PATH)
                ? value.asConfig().getConfig(CONFIG_PATH) 
                : ConfigFactory.empty();
        if (! args.isEmpty()) {
            config = ConfigValueFactory.fromMap(args).toConfig().withFallback(config);
        }

        return connect(addressFactory.get(config));
    }
    
    protected Connection connect(SocketAddress address) {
        try {
            return connections.connect(address).get();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
