package edu.uw.zookeeper.net;

import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;

public class GetConnection<C extends Connection<?,?,?>> extends ForwardingPromise<C> implements ConnectionFactory.ConnectionsListener<C> {

    public static <C extends Connection<?,?,?>> GetConnection<C> create(ConnectionFactory<C> connections) {
        return new GetConnection<C>(connections, SettableFuturePromise.<C>create());
    }
    
    protected final Promise<C> promise;
    protected final ConnectionFactory<C> connections;

    public GetConnection(
            ConnectionFactory<C> connections,
            Promise<C> promise) {
        this.promise = promise;
        this.connections = connections;
        this.connections.subscribe(this);
    }
    
    @Override
    public void handleConnectionOpen(C connection) {
        this.connections.unsubscribe(this);
        set(connection);
    }

    @Override
    protected Promise<C> delegate() {
        return promise;
    }
}