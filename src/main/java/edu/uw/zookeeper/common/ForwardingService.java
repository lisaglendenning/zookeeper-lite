package edu.uw.zookeeper.common;

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
                    public void starting() {
                    }

                    @Override
                    public void running() {
                    }

                    @Override
                    public void stopping(State from) {
                        stop();
                    }

                    @Override
                    public void terminated(State from) {
                        stop();
                    }

                    @Override
                    public void failed(State from, Throwable failure) {
                        stop();
                    }}, 
                MoreExecutors.sameThreadExecutor());
        delegate.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        delegate().stop().get();
    }

    protected abstract Service delegate();
}
