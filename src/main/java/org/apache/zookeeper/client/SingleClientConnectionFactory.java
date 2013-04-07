package org.apache.zookeeper.client;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.event.ConnectionStateEvent;
import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Configuration;

import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

public class SingleClientConnectionFactory extends ClientConnectionFactory {

    public static SingleClientConnectionFactory create(
            ClientConnectionGroup connections, Arguments arguments,
            Configuration configuration) throws Exception {
        return new SingleClientConnectionFactory(connections, arguments,
                configuration);
    }

    public static SingleClientConnectionFactory create(
            ClientConnectionGroup connections) throws Exception {
        return new SingleClientConnectionFactory(connections);
    }

    protected Connection connection;

    @Inject
    protected SingleClientConnectionFactory(ClientConnectionGroup connections,
            Arguments arguments, Configuration configuration) throws Exception {
        super(connections, arguments, configuration);
        this.connection = null;
    }

    protected SingleClientConnectionFactory(ClientConnectionGroup connections)
            throws Exception {
        super(connections);
        this.connection = null;
    }

    @Override
    public synchronized Connection get() {
        if (connection == null) {
            connection = super.get();
            connection.register(this);
        }
        return connection;
    }

    @Subscribe
    public synchronized void handleConnectionStateEvent(
            ConnectionStateEvent event) {
        if (event.connection() == connection) {
            switch (event.event()) {
            case CONNECTION_CLOSING:
            case CONNECTION_CLOSED:
                connection.unregister(this);
                connection = null;
                break;
            default:
                break;
            }
        }
    }
}
