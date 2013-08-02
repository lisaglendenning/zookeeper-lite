package edu.uw.zookeeper.common;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

public class ConfigurableSocketAddress extends AbstractConfigurableFactory<SocketAddress> {

    public static final String DEFAULT_ADDRESS = "";

    public static final String KEY_ADDRESS = "address";
    public static final String KEY_PORT = "port";

    public static ConfigurableSocketAddress create(int port) {
        return create(port, DEFAULT_ADDRESS);
    }

    public static ConfigurableSocketAddress create(int port, String address) {
        return new ConfigurableSocketAddress(port, address);
    }

    private ConfigurableSocketAddress(int port, String address) {
        super(ImmutableMap.<String, Object>builder()
                .put(KEY_PORT, port)
                .put(KEY_ADDRESS, address)
                .build());
    }
    
    @Override
    protected SocketAddress fromConfig(Config config) {
        String address = config.getString(KEY_ADDRESS);
        int port = config.getInt(KEY_PORT);
        SocketAddress socketAddress = (address == DEFAULT_ADDRESS) 
                ? new InetSocketAddress(port)
                : new InetSocketAddress(address, port);
        return socketAddress;
    }
}
