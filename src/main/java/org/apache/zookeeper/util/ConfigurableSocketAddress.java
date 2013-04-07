package org.apache.zookeeper.util;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class ConfigurableSocketAddress implements Configurable {

    public static ConfigurableSocketAddress create(String addressKey,
            String defaultAddress, String portKey, int defaultPort) {
        return new ConfigurableSocketAddress(addressKey, defaultAddress,
                portKey, defaultPort);
    }

    public static final String PARAM_DEFAULT_ADDRESS = "";

    public final Parameters.Parameter<String> PARAM_ADDRESS;
    public final Parameters.Parameter<Integer> PARAM_PORT;
    public final Parameters parameters;

    public ConfigurableSocketAddress(String addressKey, String defaultAddress,
            String portKey, int defaultPort) {
        this.PARAM_ADDRESS = Parameters
                .newParameter(addressKey, defaultAddress);
        this.PARAM_PORT = Parameters.newParameter(portKey, defaultPort);
        this.parameters = Parameters.newInstance().add(PARAM_ADDRESS)
                .add(PARAM_PORT);
    }

    @Override
    public void configure(Configuration configuration) {
        parameters.configure(configuration);
    }

    public SocketAddress socketAddress() {
        int port = PARAM_PORT.getValue();
        String address = PARAM_ADDRESS.getValue();
        SocketAddress socketAddress = (address == PARAM_DEFAULT_ADDRESS) ? new InetSocketAddress(
                port) : new InetSocketAddress(address, port);
        return socketAddress;
    }

    public void setPort(int port) {
        PARAM_PORT.setValue(port);
    }

    public int port() {
        return PARAM_PORT.getValue();
    }
}
