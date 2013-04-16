package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceApplication implements Application {

    private final ExecutorService executor;

    public ExecutorServiceApplication(ExecutorService executor) {
        this.executor = checkNotNull(executor);
    }

    public ExecutorService executor() {
        return executor;
    }

    @Override
    public Void call() throws Exception {
        executor.shutdown();
        // executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        return null;
    }
}
