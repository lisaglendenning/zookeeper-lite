package org.apache.zookeeper.client;

import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Configurable;
import org.apache.zookeeper.util.ConfigurableSocketAddress;
import org.apache.zookeeper.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;

public class ClientConnectionFactory implements Configurable, Provider<Connection> {

	public static ClientConnectionFactory create(
            ClientConnectionGroup connections,
			Arguments arguments,
            Configuration configuration) throws Exception {
		return new ClientConnectionFactory(connections, arguments, configuration);
	}

	public static ClientConnectionFactory create(
            ClientConnectionGroup connections) throws Exception {
		return new ClientConnectionFactory(connections);
	}
	
    public static final String ARG_ADDRESS = "address";
    public static final String ARG_PORT = "port";

    public static final String PARAM_KEY_ADDRESS = "Server.Address";
    public static final String PARAM_DEFAULT_ADDRESS = "localhost";
    public static final String PARAM_KEY_PORT = "Server.Port";
    public static final int PARAM_DEFAULT_PORT = 2181;
    
    protected final Logger logger = LoggerFactory.getLogger(ClientConnectionFactory.class);
    protected final ClientConnectionGroup connections;
    protected final ConfigurableSocketAddress address;
    
    @Inject
    protected ClientConnectionFactory(
            ClientConnectionGroup connections,
            Arguments arguments,
            Configuration configuration) throws Exception {
    	this(connections);
    	
        arguments.add(arguments.newOption(ARG_ADDRESS, "ServerAddress"))
            .add(arguments.newOption(ARG_PORT, "ServerPort"));
        arguments.parse();
        if (arguments.hasValue(ARG_ADDRESS)) {
            configuration.set(ClientConnectionFactory.PARAM_KEY_ADDRESS, arguments.getValue(ARG_ADDRESS));
        }
        if (arguments.hasValue(ARG_PORT)) {
            configuration.set(ClientConnectionFactory.PARAM_KEY_PORT, Integer.valueOf(arguments.getValue(ARG_PORT)));
        }
        configure(configuration);
    }

    protected ClientConnectionFactory(
            ClientConnectionGroup connections) throws Exception {
        this.connections = connections;
        this.address = ConfigurableSocketAddress.create(PARAM_KEY_ADDRESS,
                PARAM_DEFAULT_ADDRESS, PARAM_KEY_PORT, PARAM_DEFAULT_PORT);
    }
    
    @Override
    public void configure(Configuration configuration) {
        address.configure(configuration);
    }

	@Override
    public Connection get() {
		Connection connection = null;
    	SocketAddress server = address.socketAddress();
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
