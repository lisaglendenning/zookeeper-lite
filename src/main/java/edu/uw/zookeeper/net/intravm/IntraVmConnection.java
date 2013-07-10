package edu.uw.zookeeper.net.intravm;


import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.util.Actor;
import edu.uw.zookeeper.util.Automatons;
import edu.uw.zookeeper.util.ForwardingPromise;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class IntraVmConnection implements Connection<Object> {
    
    public static <I> Pair<IntraVmConnection, IntraVmConnection> createPair(
            Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint> endpoints) {
        return Pair.create(
                IntraVmConnection.create(endpoints.first(), endpoints.second()),
                IntraVmConnection.create(endpoints.second(), endpoints.first()));
    }
    
    public static IntraVmConnection create(IntraVmConnectionEndpoint local, IntraVmConnectionEndpoint remote) {
        return new IntraVmConnection(local, remote);
    }
    
    protected final Automatons.SynchronizedEventfulAutomaton<Connection.State, Connection.State> state;
    protected final IntraVmConnectionEndpoint localEndpoint;
    protected final IntraVmConnectionEndpoint remoteEndpoint;
    protected final CloseTask closeTask;
    
    public IntraVmConnection(
            IntraVmConnectionEndpoint localActor,
            IntraVmConnectionEndpoint remoteActor) {
        this.state = Automatons.createSynchronizedEventful(this, 
                Automatons.createSimple(Connection.State.CONNECTION_OPENING));
        this.localEndpoint = localActor;
        this.remoteEndpoint = remoteActor;
        this.closeTask = new CloseTask();
        
        localEndpoint.stopped().addListener(closeTask, this);
        
        state.apply(Connection.State.CONNECTION_OPENED);
    }
    
    @Override
    public Connection.State state() {
        return state.state();
    }

    @Override
    public IntraVmSocketAddress localAddress() {
        return localEndpoint.address();
    }

    @Override
    public IntraVmSocketAddress remoteAddress() {
        return remoteEndpoint.address();
    }

    @Override
    public void read() {
        Connection.State state = state();
        switch (state) {
        case CONNECTION_CLOSING:
        case CONNECTION_CLOSED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        
        localEndpoint.schedule();
    }

    @Override
    public <T extends Object> ListenableFuture<T> write(T message) {
        Connection.State state = state();
        switch (state) {
        case CONNECTION_CLOSING:
        case CONNECTION_CLOSED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        
        try {
            remoteEndpoint.send(Optional.of((Object) message));
        } catch (Exception e) {
            close();
            return Futures.immediateFailedCheckedFuture(e);
        }
        return Futures.immediateFuture(message);
    }

    @Override
    public ListenableFuture<Connection<Object>> close() {
        if (Connection.State.CONNECTION_CLOSING == state.apply(Connection.State.CONNECTION_CLOSING).orNull()) {
            execute(closeTask);
        }
        return closeTask;
    }

    @Override
    public void execute(Runnable command) {
        localEndpoint.execute(command);
    }

    @Override
    public void post(Object object) {
        localEndpoint.post(object);
    }

    @Override
    public void register(Object object) {
        localEndpoint.register(object);
    }

    @Override
    public void unregister(Object object) {
        localEndpoint.unregister(object);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(String.format("%s => %s", localAddress(), remoteAddress()))
                .toString();
    }
    
    protected class CloseTask extends ForwardingPromise<Connection<Object>> implements Runnable {

        protected final Promise<Connection<Object>> delegate;
        
        protected CloseTask() {
            this.delegate = SettableFuturePromise.create();
        }
        
        @Override
        protected Promise<Connection<Object>> delegate() {
            return delegate;
        }
        
        public void run() {
            state.apply(Connection.State.CONNECTION_CLOSING);
            
            localEndpoint.stop();

            try {
                if (Actor.State.TERMINATED != remoteEndpoint.state()) {
                    remoteEndpoint.send(Optional.<Object>absent());
                }
            } catch (Exception e) {}
            
            if (Actor.State.TERMINATED == localEndpoint.state()) {
                state.apply(Connection.State.CONNECTION_CLOSED);
                if (! isDone()) {
                    try {
                        localEndpoint.stopped().get();
                        set(IntraVmConnection.this);
                    } catch (Exception e) {
                        setException(e);
                    }
                }
            }
        }
    }
}
