package edu.uw.zookeeper.net;

import java.net.SocketAddress;

import com.google.common.util.concurrent.ListenableFuture;

public abstract class ForwardingConnection<I> implements Connection<I> {

    @Override
    public void execute(Runnable runnable) {
        delegate().execute(runnable);
    }
    
    @Override
    public void post(Object object) {
        delegate().post(object);
    }

    @Override
    public void register(Object object) {
        delegate().register(object);
    }

    @Override
    public void unregister(Object object) {
        delegate().unregister(object);
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

    @Override
    public void read() {
        delegate().read();
    }

    @Override
    public ListenableFuture<I> write(I message) {
        return delegate().write(message);
    }

    @Override
    public ListenableFuture<Connection<I>> flush() {
        return delegate().flush();
    }

    @Override
    public ListenableFuture<Connection<I>> close() {
        return delegate().close();
    }

    protected abstract Connection<I> delegate();
    
    @Override
    public String toString() {
        return delegate().toString();
    }
}
