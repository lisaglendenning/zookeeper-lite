package edu.uw.zookeeper.common;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;

public abstract class ForwardingService extends AbstractIdleService {

    @Override
    protected Executor executor() {
        return SameThreadExecutor.getInstance();
    }
    
    @Override
    protected void startUp() throws Exception {
        startService(delegate());
    }

    @Override
    protected void shutDown() throws Exception {
        delegate().stopAsync().awaitTerminated();
    }
    
    protected void startService(Service service) throws ExecutionException {
        service.addListener(
                new Listener(), 
                SameThreadExecutor.getInstance());
        switch (service.state()) {
        case NEW:
            service.startAsync();
        case STARTING:
            service.awaitRunning();
        case RUNNING:
            break;
        case STOPPING:
        case TERMINATED:
            throw new IllegalStateException(String.valueOf(service));
        case FAILED:
            throw new ExecutionException(service.failureCause());
        }
    }

    protected abstract Service delegate();
    
    protected class Listener extends Service.Listener {
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
        }
    }
}
