package edu.uw.zookeeper.common;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

public abstract class ForwardingService extends AbstractIdleService {

    @Override
    protected Executor executor() {
        return MoreExecutors.sameThreadExecutor();
    }
    
    @Override
    protected void startUp() throws Exception {
        Service delegate = delegate();
        delegate.addListener(
                new Listener() {
                    @Override
                    public void stopping(State from) {
                        stopAsync();
                    }

                    @Override
                    public void terminated(State from) {
                        stopAsync();
                    }

                    @Override
                    public void failed(State from, Throwable failure) {
                        stopAsync();
                    }}, 
                MoreExecutors.sameThreadExecutor());
        switch (delegate.state()) {
        case NEW:
            delegate.startAsync();
        case STARTING:
            delegate.awaitRunning();
        case RUNNING:
            break;
        case STOPPING:
        case TERMINATED:
            throw new IllegalStateException();
        case FAILED:
            throw new ExecutionException(delegate.failureCause());
        }
    }

    @Override
    protected void shutDown() throws Exception {
        delegate().stopAsync();
        delegate().awaitTerminated();
    }

    protected abstract Service delegate();
}
