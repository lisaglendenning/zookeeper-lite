package org.apache.zookeeper.netty.server;

import java.net.SocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.group.ChannelGroup;

import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Configurable;
import org.apache.zookeeper.util.ConfigurableSocketAddress;
import org.apache.zookeeper.util.Configuration;
import org.apache.zookeeper.util.Eventful;

import com.google.inject.Inject;

public class ConfigurableChannelServerConnectionGroup extends ChannelServerConnectionGroup implements Configurable {

    public static ConfigurableChannelServerConnectionGroup create(
            Arguments arguments,
            Configuration configuration,
            Eventful eventful,
            ServerConnection.Factory connectionFactory,
            ChannelGroup channels,
            ServerBootstrap bootstrap
            ) throws Exception {
        return new ConfigurableChannelServerConnectionGroup(
                arguments, configuration, eventful, connectionFactory, channels, bootstrap);
    }
    
    public static ConfigurableChannelServerConnectionGroup create(
            Eventful eventful,
            ServerConnection.Factory connectionFactory,
            ChannelGroup channels,
            ServerBootstrap bootstrap) {
        return new ConfigurableChannelServerConnectionGroup(
                eventful, connectionFactory, channels, bootstrap);
    }

    public static final String ARG_ADDRESS = "clientAddress";
    public static final String ARG_PORT = "clientPort";
    
    public static final String PARAM_KEY_ADDRESS = "Client.Address";
    public static final String PARAM_DEFAULT_ADDRESS = "";
    
    public static final String PARAM_KEY_PORT = "Client.Port";
    public static final int PARAM_DEFAULT_PORT = 2181;
    
    protected final ConfigurableSocketAddress address;

    @Inject
    protected ConfigurableChannelServerConnectionGroup(
            Arguments arguments,
            Configuration configuration,
            Eventful eventful,
            ServerConnection.Factory connectionFactory,
            ChannelGroup channels,
            ServerBootstrap bootstrap
            ) throws Exception {
        this(eventful, connectionFactory, channels, bootstrap);
        
        arguments.add(arguments.newOption(ARG_ADDRESS, "ClientAddress"))
            .add(arguments.newOption(ARG_PORT, "ClientPort"));
        arguments.parse();
        if (arguments.hasValue(ARG_ADDRESS)) {
            configuration.set(PARAM_KEY_ADDRESS, arguments.getValue(ARG_ADDRESS));
        }
        if (arguments.hasValue(ARG_PORT)) {
            configuration.set(PARAM_KEY_PORT, Integer.valueOf(arguments.getValue(ARG_PORT)));
        }
        configure(configuration);
    }
    
    protected ConfigurableChannelServerConnectionGroup(
            Eventful eventful,
            ServerConnection.Factory connectionFactory,
            ChannelGroup channels,
            ServerBootstrap bootstrap) {
        super(eventful, connectionFactory, channels, bootstrap);
        this.address = ConfigurableSocketAddress.create(PARAM_KEY_ADDRESS,
                PARAM_DEFAULT_ADDRESS, PARAM_KEY_PORT, PARAM_DEFAULT_PORT);
    }

    @Override
    public void configure(Configuration configuration) {
        address.configure(configuration);
        serverBootstrap().localAddress(address.socketAddress());
    }

    @Override
    public SocketAddress localAddress() {
        SocketAddress socketAddress = super.localAddress();
        if (socketAddress == null) {
            socketAddress = address.socketAddress();
        }
        return socketAddress;
    }

}
