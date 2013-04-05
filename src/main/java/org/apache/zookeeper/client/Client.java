package org.apache.zookeeper.client;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Configurable;
import org.apache.zookeeper.util.ConfigurableSocketAddress;
import org.apache.zookeeper.util.Configuration;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class Client extends AbstractIdleService implements Configurable {

    public static class Factory implements Provider<Client> {

        public static final String ARG_ADDRESS = "address";
        public static final String ARG_PORT = "port";
        
        protected final Configuration configuration;
        protected final ClientConnectionGroup connections;
        protected final ClientSessionConnection.Factory sessionFactory;
        
        @Inject
        public Factory(Arguments arguments,
                Configuration configuration,
                ClientConnectionGroup connections,
                ClientSessionConnection.Factory sessionFactory) throws Exception {
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
            this.sessionFactory = sessionFactory;
        }
        
        public Client get() {
            return Client.create(configuration, connections, sessionFactory);
        }
    }
    
    public static Client create(Configuration configuration, 
            ClientConnectionGroup connections,
            ClientSessionConnection.Factory sessionFactory) {
        return new Client(configuration, connections, sessionFactory);
    }
    
    public static Client create(
            ClientConnectionGroup connections,
            ClientSessionConnection.Factory sessionFactory) {
        return new Client(connections, sessionFactory);
    }
    
    public static final String PARAM_KEY_ADDRESS = "Client.Address";
    public static final String PARAM_DEFAULT_ADDRESS = "localhost";
    public static final String PARAM_KEY_PORT = "Client.Port";
    public static final int PARAM_DEFAULT_PORT = 2180;
    
    protected final ConfigurableSocketAddress address;
    protected final ClientConnectionGroup connections;
    protected final ClientSessionConnection.Factory sessionFactory;
    protected ClientSessionConnection session;
    
    @Inject
    protected Client(Configuration configuration, 
            ClientConnectionGroup connections,
            ClientSessionConnection.Factory sessionFactory) {
        this(connections, sessionFactory);
        configure(configuration);
    }

    protected Client(
            ClientConnectionGroup connections,
            ClientSessionConnection.Factory sessionFactory) {
        this.address = ConfigurableSocketAddress.create(PARAM_KEY_ADDRESS,
                PARAM_DEFAULT_ADDRESS, PARAM_KEY_PORT, PARAM_DEFAULT_PORT);
        this.connections = connections;
        this.sessionFactory = sessionFactory;
        this.session = null;
    }
    
    public ClientSessionConnection session() {
        return session;
    }
    
    @Override
    public void configure(Configuration configuration) {
        address.configure(configuration);
    }

    @Override
    protected void startUp() throws Exception {
        Connection connection = connections.connect(address.socketAddress()).get();
        this.session = sessionFactory.get(connection);
        session.connect().get();
    }

    @Override
    protected void shutDown() throws Exception {
        session.disconnect().get();
    }
}
