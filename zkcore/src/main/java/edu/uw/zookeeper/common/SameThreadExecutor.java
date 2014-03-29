package edu.uw.zookeeper.common;

import java.util.concurrent.Executor;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.MoreExecutors;

public enum SameThreadExecutor implements Supplier<Executor> {
    SAME_THREAD_EXECUTOR;
    
    public static Executor getInstance() {
        return SAME_THREAD_EXECUTOR.instance;
    }
    
    private final Executor instance = MoreExecutors.sameThreadExecutor();
    
    @Override
    public Executor get() {
        return instance;
    }
}
