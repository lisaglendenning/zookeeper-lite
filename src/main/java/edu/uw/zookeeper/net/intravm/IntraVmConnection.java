package edu.uw.zookeeper.net.intravm;


import java.net.SocketAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.RunnablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;

public class IntraVmConnection<V> implements Connection<V> {

    public static <V> IntraVmConnection<V> create(
            IntraVmEndpoint<?> local, IntraVmEndpoint<?> remote) {
        return new IntraVmConnection<V>(local, remote);
    }

    protected final Logger logger;
    protected final Automatons.SynchronizedEventfulAutomaton<Connection.State, Connection.State> state;
    protected final IntraVmEndpoint<?> local;
    protected final IntraVmEndpoint<?> remote;
    protected final CloseTask closeTask;
    
    protected IntraVmConnection(
            IntraVmEndpoint<?> local,
            IntraVmEndpoint<?> remote) {
        this.logger = LogManager.getLogger(getClass());
        this.state = Automatons.createSynchronizedEventful(this, 
                Automatons.createSimple(Connection.State.CONNECTION_OPENED));
        this.local = local;
        this.remote = remote;
        this.closeTask = new CloseTask();
    }
    
    protected IntraVmEndpoint<?> local() {
        return local;
    }
    
    protected IntraVmEndpoint<?> remote() {
        return remote;
    }
    
    @Override
    public Connection.State state() {
        return state.state();
    }

    @Override
    public SocketAddress localAddress() {
        return local.address();
    }

    @Override
    public SocketAddress remoteAddress() {
        return remote.address();
    }

    @Override
    public void read() {
        local.run();
    }

    @Override
    public <U extends V> ListenableFuture<U> write(U message) {
        Connection.State state = state();
        switch (state) {
        case CONNECTION_CLOSING:
        case CONNECTION_CLOSED:
            return Futures.immediateFailedFuture(new IllegalStateException(state.toString()));
        default:
            break;
        }
        
        return local.write(Optional.of(message), remote);
    }

    @Override
    public void flush() {
        remote.run();
    }

    @Override
    public ListenableFuture<IntraVmConnection<V>> close() {
        if (state().compareTo(Connection.State.CONNECTION_CLOSING) < 0) {
            if (Connection.State.CONNECTION_CLOSING == state.apply(Connection.State.CONNECTION_CLOSING).orNull()) {
                execute(closeTask);
            }
        }
        return closeTask;
    }

    @Override
    public void execute(Runnable command) {
        local.execute(command);
    }

    @Override
    public void post(Object object) {
        local.post(object);
    }

    @Override
    public void register(Object object) {
        local.register(object);
    }

    @Override
    public void unregister(Object object) {
        local.unregister(object);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(String.format("%s => %s", localAddress(), remoteAddress()))
                .toString();
    }
    
    protected class CloseTask extends RunnablePromiseTask<IntraVmConnection<V>, IntraVmConnection<V>> implements Runnable {

        protected CloseTask() {
            super(IntraVmConnection.this, 
                    LoggingPromise.create(
                            logger, 
                            SettableFuturePromise.<IntraVmConnection<V>>create()));
            local.stopped().addListener(this, IntraVmConnection.this);
        }
        
        @Override
        protected Promise<IntraVmConnection<V>> delegate() {
            return delegate;
        }

        @Override
        public synchronized Optional<IntraVmConnection<V>> call() throws Exception {
            logger.entry(this);
            
            if (state().compareTo(Connection.State.CONNECTION_CLOSING) < 0) {
                state.apply(Connection.State.CONNECTION_CLOSING);
            }

            if (remote.state().compareTo(Actor.State.TERMINATED) < 0) {
                local.write(Optional.<V>absent(), remote);
                local.run();
            }

            if (local.state().compareTo(Actor.State.TERMINATED) < 0) {
                local.stop();
            }

            if (local.stopped().isDone()) {
                try {
                    if (! isDone()) {
                        local.stopped().get();
                        return Optional.of(task());
                    }
                } finally {
                    if (state().compareTo(Connection.State.CONNECTION_CLOSED) < 0) {
                        state.apply(Connection.State.CONNECTION_CLOSED);
                    }
                }
            }
            
            return logger.exit(Optional.<IntraVmConnection<V>>absent());
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("future", delegate())
                    .add("this", IntraVmConnection.this)
                    .add("state", state())
                    .add("local", local)
                    .add("remote", remote)
                    .toString();
        }
    }
}
