package edu.uw.zookeeper.util;

import java.util.concurrent.Executor;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

public class ForwardingService implements Service {
    protected final Service service;

    protected ForwardingService(Service service) {
        this.service = service;
    }

    protected Service delegate() {
        return service;
    }

    @Override
    public void addListener(Listener arg0, Executor arg1) {
        delegate().addListener(arg0, arg1);
    }

    @Override
    public Throwable failureCause() {
        return delegate().failureCause();
    }

    @Override
    public boolean isRunning() {
        return delegate().isRunning();
    }

    @Override
    public ListenableFuture<State> start() {
        return delegate().start();
    }

    @Override
    public State startAndWait() {
        return delegate().startAndWait();
    }

    @Override
    public State state() {
        return delegate().state();
    }

    @Override
    public ListenableFuture<State> stop() {
        return delegate().stop();
    }

    @Override
    public State stopAndWait() {
        return delegate().stopAndWait();
    }
}
