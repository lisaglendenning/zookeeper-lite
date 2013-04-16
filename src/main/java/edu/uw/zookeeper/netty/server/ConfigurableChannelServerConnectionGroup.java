package edu.uw.zookeeper.netty.server;

import java.net.SocketAddress;
import java.util.Map;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.group.ChannelGroup;


import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigValueFactory;

import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Configurable;
import edu.uw.zookeeper.util.ConfigurableSocketAddress;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Eventful;

public class ConfigurableChannelServerConnectionGroup extends
        ChannelServerConnectionGroup implements Configurable {

    public static ConfigurableChannelServerConnectionGroup create(
            Arguments arguments, Configuration configuration,
            Eventful eventful, ServerConnection.Factory connectionFactory,
            ChannelGroup channels, ServerBootstrap bootstrap) throws Exception {
        return new ConfigurableChannelServerConnectionGroup(arguments,
                configuration, eventful, connectionFactory, channels, bootstrap);
    }

    public static ConfigurableChannelServerConnectionGroup create(
            Eventful eventful, ServerConnection.Factory connectionFactory,
            ChannelGroup channels, ServerBootstrap bootstrap) {
        return new ConfigurableChannelServerConnectionGroup(eventful,
                connectionFactory, channels, bootstrap);
    }

    public static final String ARG_ADDRESS = "clientAddress";
    public static final String ARG_PORT = "clientPort";

    public static final String CONFIG_PATH = "Client.Address";
    public static final String PARAM_DEFAULT_ADDRESS = "";
    public static final int PARAM_DEFAULT_PORT = 2181;

    protected final ConfigurableSocketAddress address;

    @Inject
    protected ConfigurableChannelServerConnectionGroup(Arguments arguments,
            Configuration configuration, Eventful eventful,
            ServerConnection.Factory connectionFactory, ChannelGroup channels,
            ServerBootstrap bootstrap) throws Exception {
        this(configuration, eventful, connectionFactory, channels, bootstrap);

        arguments.add(arguments.newOption(ARG_ADDRESS, "ClientAddress")).add(
                arguments.newOption(ARG_PORT, "ClientPort"));
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
            serverBootstrap().localAddress(address.get());
        }
    }

    protected ConfigurableChannelServerConnectionGroup(
            Configuration configuration,
            Eventful eventful,
            ServerConnection.Factory connectionFactory, ChannelGroup channels,
            ServerBootstrap bootstrap) {
        this(eventful, connectionFactory, channels, bootstrap);
        configure(configuration);
    }

    protected ConfigurableChannelServerConnectionGroup(Eventful eventful,
            ServerConnection.Factory connectionFactory, ChannelGroup channels,
            ServerBootstrap bootstrap) {
        super(eventful, connectionFactory, channels, bootstrap);
        this.address = ConfigurableSocketAddress.create(
                PARAM_DEFAULT_PORT, PARAM_DEFAULT_ADDRESS);
        serverBootstrap().localAddress(address.get());
    }

    @Override
    public void configure(Configuration configuration) {
        try {
            address.get(configuration.get().getConfig(CONFIG_PATH));
            serverBootstrap().localAddress(address.get());
        } catch (ConfigException.Missing e) {}
    }

    @Override
    public SocketAddress localAddress() {
        SocketAddress socketAddress = super.localAddress();
        if (socketAddress == null) {
            socketAddress = address.get();
        }
        return socketAddress;
    }

}
