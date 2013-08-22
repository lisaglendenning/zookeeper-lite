package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

public class RunnableService extends AbstractExecutionThreadService {

    public static RunnableService create(Runnable runnable) {
        return new RunnableService(runnable);
    }
    
    protected final Runnable runnable;
    
    public RunnableService(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    protected void run() {
        runnable.run();
    }
}
