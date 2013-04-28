package edu.uw.zookeeper.net;

import java.net.SocketAddress;
import java.util.Map;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.ConfigurableSocketAddress;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class ConfigurableServerConnectionFactoryBuilder
        implements DefaultsFactory<Configuration, ServerConnectionFactory> {

    public static ConfigurableServerConnectionFactoryBuilder create(
            ParameterizedFactory<SocketAddress, ServerConnectionFactory> factoryBuilder) {
        return create(
                factoryBuilder,
                ConfigurableSocketAddress.create(DEFAULT_PORT));
    }

    public static ConfigurableServerConnectionFactoryBuilder create(
            ParameterizedFactory<SocketAddress, ServerConnectionFactory> factoryBuilder,
            DefaultsFactory<Config, SocketAddress> addressFactory) {
        return new ConfigurableServerConnectionFactoryBuilder(
                factoryBuilder,
                addressFactory);
    }

    public static final String ARG_ADDRESS = "clientAddress";
    public static final String ARG_PORT = "clientPort";

    public static final String CONFIG_PATH = "Client.Address";
    public static final int DEFAULT_PORT = 2181;

    private final ParameterizedFactory<SocketAddress, ServerConnectionFactory> factoryBuilder;
    private final DefaultsFactory<Config, SocketAddress> addressFactory;

    private ConfigurableServerConnectionFactoryBuilder(
            ParameterizedFactory<SocketAddress, ServerConnectionFactory> factoryBuilder,
            DefaultsFactory<Config, SocketAddress> addressFactory) {
        this.factoryBuilder = factoryBuilder;
        this.addressFactory = addressFactory;
    }

    @Override
    public ServerConnectionFactory get() {
        SocketAddress address = addressFactory.get();
        return factoryBuilder.get(address);
    }

    @Override
    public ServerConnectionFactory get(Configuration value) {
        Arguments arguments = value.asArguments();
        if (! arguments.has(ARG_ADDRESS)) {
            arguments.add(arguments.newOption(ARG_ADDRESS, "ClientAddress"));
        }
        if (! arguments.has(ARG_PORT)) {
            arguments.add(arguments.newOption(ARG_PORT, "ClientPort"));
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
        SocketAddress address = addressFactory.get(config);
        return factoryBuilder.get(address);
    }
}
