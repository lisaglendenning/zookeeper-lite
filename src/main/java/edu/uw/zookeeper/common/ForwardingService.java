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
        
        if (delegate.state() == State.FAILED) {
            throw new ExecutionException(delegate.failureCause());
        } else if (delegate.state() == State.TERMINATED) {
            throw new IllegalStateException();
        }
        
        delegate.startAsync();
        delegate.awaitRunning();
    }

    @Override
    protected void shutDown() throws Exception {
        delegate().stopAsync();
        delegate().awaitTerminated();
    }

    protected abstract Service delegate();
}
