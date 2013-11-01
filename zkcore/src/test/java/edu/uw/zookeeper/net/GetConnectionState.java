package edu.uw.zookeeper.net;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;

public class GetConnectionState extends ForwardingPromise<Automaton.Transition<Connection.State>> implements Connection.Listener<Object> {

    public static GetConnectionState create(Connection<?,?,?> connection) {
        return new GetConnectionState(connection, SettableFuturePromise.<Automaton.Transition<Connection.State>>create());
    }
    
    protected final Promise<Automaton.Transition<Connection.State>> promise;
    protected final Connection<?,?,?> connection;

    public GetConnectionState(
            Connection<?,?,?> connection,
            Promise<Automaton.Transition<Connection.State>> promise) {
        this.promise = promise;
        this.connection = connection;
        this.connection.subscribe(this);
    }
    
    @Override
    protected Promise<Automaton.Transition<Connection.State>> delegate() {
        return promise;
    }

    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> state) {
        connection.unsubscribe(this);
        set(state);
    }

    @Override
    public void handleConnectionRead(Object message) {
    }
}