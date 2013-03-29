package org.apache.zookeeper.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceApplication implements Application {

    protected final ExecutorService executor;
    
    public ExecutorServiceApplication(ExecutorService executor) {
        this.executor = executor;
    }
    
    public ExecutorService executor() {
        return executor;
    }
    
    @Override
    public Void call() throws Exception {
        executor.shutdown();
        //executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        return null;
    }
}
