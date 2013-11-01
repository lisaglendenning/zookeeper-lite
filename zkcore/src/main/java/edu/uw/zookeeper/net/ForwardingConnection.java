package edu.uw.zookeeper.net;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

public abstract class ForwardingConnection<I, O, T extends Connection<? super I, ? extends O, ?>, C extends ForwardingConnection<I,O,T,C>> implements Connection<I,O,C> {

    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final Function<Object, C> RETURN_SELF;
    
    @SuppressWarnings("unchecked")
    protected ForwardingConnection() {
        this.RETURN_SELF = Functions.constant((C) this);
    }
    
    @Override
    public void execute(Runnable runnable) {
        delegate().execute(runnable);
    }
    
    @Override
    public void subscribe(Listener<? super O> listener) {
        delegate().subscribe(listener);
    }

    @Override
    public boolean unsubscribe(Listener<? super O> listener) {
        return delegate().unsubscribe(listener);
    }

    @Override
    public Connection.State state() {
        return delegate().state();
    }

    @Override
    public SocketAddress localAddress() {
        return delegate().localAddress();
    }

    @Override
    public SocketAddress remoteAddress() {
        return delegate().remoteAddress();
    }

    @SuppressWarnings("unchecked")
    @Override
    public C read() {
        delegate().read();
        return (C) this;
    }

    @Override
    public <I1 extends I> ListenableFuture<I1> write(I1 message) {
        return delegate().write(message);
    }

    @SuppressWarnings("unchecked")
    @Override
    public C flush() {
        delegate().flush();
        return (C) this;
    }

    @Override
    public ListenableFuture<? extends C> close() {
        return Futures.transform(
                delegate().close(), RETURN_SELF, SAME_THREAD_EXECUTOR);
    }

    @Override
    public String toString() {
        return delegate().toString();
    }

    protected abstract T delegate();
}
