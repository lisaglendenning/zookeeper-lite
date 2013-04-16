package edu.uw.zookeeper.util;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

public class ConfigurableSocketAddress extends ConfigurableReference<SocketAddress> {

    public static ConfigurableSocketAddress create(int port, String address) {
        Factory factory = Factory.create(port, address);
        return new ConfigurableSocketAddress(factory);
    }

    public static ConfigurableSocketAddress create(int port) {
        Factory factory = Factory.create(port);
        return new ConfigurableSocketAddress(factory);
    }

    protected ConfigurableSocketAddress(ConfigurableFactory<SocketAddress> configurable) {
        super(configurable);
    }
    
    public static class Factory extends AbstractConfigurableFactory<SocketAddress> {

        public static final String DEFAULT_ADDRESS = "";

        public static final String KEY_ADDRESS = "address";
        public static final String KEY_PORT = "port";

        public static Factory create(int port) {
            return new Factory(port, DEFAULT_ADDRESS);
        }

        public static Factory create(int port, String address) {
            return new Factory(port, address);
        }

        protected Factory(int port, String address) {
            super(ImmutableMap.<String, Object>builder()
                    .put(KEY_PORT, port)
                    .put(KEY_ADDRESS, address)
                    .build());
        }
        
        @Override
        public SocketAddress get(Config config) {
            if (config != defaults) {
                config = config.withFallback(defaults);
            }
            String address = config.getString(KEY_ADDRESS);
            int port = config.getInt(KEY_PORT);
            SocketAddress socketAddress = (address == DEFAULT_ADDRESS) 
                    ? new InetSocketAddress(port)
                    : new InetSocketAddress(address, port);
            return socketAddress;
        }
    }
}
