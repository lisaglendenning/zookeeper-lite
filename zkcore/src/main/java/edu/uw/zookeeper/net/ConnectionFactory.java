package edu.uw.zookeeper.net;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Eventful;

/**
 * Collection of Connections.
 */
public interface ConnectionFactory<C extends Connection<?,?,?>> extends Iterable<C>, Eventful<ConnectionFactory.ConnectionsListener<? super C>>, Service {
    public static interface ConnectionsListener<C extends Connection<?,?,?>> {
        void handleConnectionOpen(C connection);
    }
}
