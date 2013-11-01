package edu.uw.zookeeper.net;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;

public class GetConnectionRead<O> extends ForwardingPromise<O> implements Connection.Listener<O> {

    public static <O> GetConnectionRead<O> create(Connection<?,? extends O,?> connection) {
        return new GetConnectionRead<O>(connection, SettableFuturePromise.<O>create());
    }
    
    protected final Promise<O> promise;
    protected final Connection<?,? extends O,?> connection;

    public GetConnectionRead(
            Connection<?,? extends O,?> connection,
            Promise<O> promise) {
        this.promise = promise;
        this.connection = connection;
        this.connection.subscribe(this);
    }
    
    @Override
    protected Promise<O> delegate() {
        return promise;
    }

    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> state) {
    }

    @Override
    public void handleConnectionRead(O message) {
        connection.unsubscribe(this);
        set(message);
    }
}