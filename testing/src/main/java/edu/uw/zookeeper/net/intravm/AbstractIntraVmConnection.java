package edu.uw.zookeeper.net.intravm;


import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;

public abstract class AbstractIntraVmConnection<I,O,U,V, T extends AbstractIntraVmEndpoint<I,O,U,V>, C extends AbstractIntraVmConnection<I,O,U,V,T,C>> implements Connection<I,O,C> {

    protected final Logger logger;
    protected final T local;
    protected final AbstractIntraVmEndpoint<?,?,?,? super U> remote;
    protected final CallablePromiseTask<Close,C> close;
    
    protected AbstractIntraVmConnection(
            T local,
            AbstractIntraVmEndpoint<?,?,?,? super U> remote) {
        this.logger = LogManager.getLogger(getClass());
        this.local = local;
        this.remote = remote;
        this.close = CallablePromiseTask.create(
                new Close(), 
                SettableFuturePromise.<C>create());
        LoggingFutureListener.listen(logger, close);
    }
    
    protected T local() {
        return local;
    }
    
    protected AbstractIntraVmEndpoint<?,?,?,? super U> remote() {
        return remote;
    }
    
    @Override
    public Connection.State state() {
        return local.state();
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
    public void subscribe(Connection.Listener<? super O> listener) {
        local.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(Connection.Listener<? super O> listener) {
        return local.unsubscribe(listener);
    }

    @Override
    public C read() {
        local.reader().run();
        return self();
    }

    @Override
    public <I1 extends I> ListenableFuture<I1> write(I1 message) {
        return local.writer().submit(message, remote);
    }

    @Override
    public C flush() {
        local.writer().run();
        return self();
    }

    @Override
    public ListenableFuture<? extends C> close() {
        if (!close.isDone()) {
            execute(close);
        }
        return close;
    }

    @Override
    public void execute(Runnable command) {
        local.execute(command);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .addValue(String.format("%s => %s", localAddress(), remoteAddress()))
                .toString();
    }
    
    @SuppressWarnings("unchecked")
    protected C self() {
        return (C) this;
    }
    
    protected class Close implements Callable<Optional<C>>, Connection.Listener<Object> {

        public Close() {
        }
        
        @Override
        public Optional<C> call() throws IOException {
            logger.entry(this);
            Optional<C> result = Optional.absent();
            
            switch (local.state()) {
            case CONNECTION_CLOSED:
                break;
            case CONNECTION_CLOSING:
                local.subscribe(this);
                break;
            case CONNECTION_OPENED:
            case CONNECTION_OPENING:
                local.subscribe(this);
                local.close();
                break;
            default:
                break;
            }
            
            if (local.state() == Connection.State.CONNECTION_CLOSED) {
                local.unsubscribe(this);
                result = Optional.of(self());
            }
            
            switch (remote.state()) {
            case CONNECTION_CLOSED:
                break;
            case CONNECTION_CLOSING:
            case CONNECTION_OPENED:
            case CONNECTION_OPENING:
                remote.close();
                break;
            default:
                break;
            }

            return logger.exit(result);
        }

        @Override
        public void handleConnectionState(
                Automaton.Transition<Connection.State> state) {
            if (state.to() == Connection.State.CONNECTION_CLOSED) {
                close();
            }
        }

        @Override
        public void handleConnectionRead(Object message) {
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(state()).toString();
        }
    }
}
