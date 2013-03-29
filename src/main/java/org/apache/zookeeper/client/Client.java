package org.apache.zookeeper.client;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Configurable;
import org.apache.zookeeper.util.ConfigurableSocketAddress;
import org.apache.zookeeper.util.Configuration;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

public class Client extends AbstractIdleService implements Configurable {

    public static class Factory {

        public static final String ARG_ADDRESS = "address";
        public static final String ARG_PORT = "port";
        
        protected Configuration configuration;
        protected ClientConnectionGroup connections;
        
        @Inject
        public Factory(Arguments arguments,
                Configuration configuration,
                ClientConnectionGroup connections) throws Exception {
            arguments.add(arguments.newOption(ARG_ADDRESS, "ServerAddress"))
                .add(arguments.newOption(ARG_PORT, "ServerPort"));
            arguments.parse();
            if (arguments.hasValue(ARG_ADDRESS)) {
                configuration.set(Client.PARAM_KEY_ADDRESS, arguments.getValue(ARG_ADDRESS));
            }
            if (arguments.hasValue(ARG_PORT)) {
                configuration.set(Client.PARAM_KEY_PORT, Integer.valueOf(arguments.getValue(ARG_PORT)));
            }
            this.configuration = configuration;
            this.connections = connections;
        }
        
        public Client newClient(ClientSession session) {
            return Client.create(configuration, connections, session);
        }
    }
    
    public static Client create(Configuration configuration, 
            ClientConnectionGroup connections,
            ClientSession session) {
        return new Client(configuration, connections, session);
    }
    
    public static Client create(
            ClientConnectionGroup connections,
            ClientSession session) {
        return new Client(connections, session);
    }
    
    public static final String PARAM_KEY_ADDRESS = "Client.Address";
    public static final String PARAM_DEFAULT_ADDRESS = "localhost";
    public static final String PARAM_KEY_PORT = "Client.Port";
    public static final int PARAM_DEFAULT_PORT = 2180;
    
    protected final ConfigurableSocketAddress address;
    protected ClientConnectionGroup connections;
    protected ClientSession session;
    
    @Inject
    protected Client(Configuration configuration, 
            ClientConnectionGroup connections,
            ClientSession session) {
        this(connections, session);
        configure(configuration);
    }

    protected Client(
            ClientConnectionGroup connections,
            ClientSession session) {
        this.address = ConfigurableSocketAddress.create(PARAM_KEY_ADDRESS,
                PARAM_DEFAULT_ADDRESS, PARAM_KEY_PORT, PARAM_DEFAULT_PORT);
        this.connections = connections;
        this.session = session;
    }
    
    public ClientSession session() {
        return session;
    }
    
    @Override
    public void configure(Configuration configuration) {
        address.configure(configuration);
    }

    @Override
    protected void startUp() throws Exception {
        Connection connection = connections.connect(address.socketAddress()).get();
        session.connect(connection).get();
    }

    @Override
    protected void shutDown() throws Exception {
        Connection connection = session.connection();
        session.close().get();
        connection.close();
    }
    
}
