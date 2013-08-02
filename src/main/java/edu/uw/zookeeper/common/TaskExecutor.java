package edu.uw.zookeeper.common;

import com.google.common.util.concurrent.ListenableFuture;


public interface TaskExecutor<I,O> {
    /**
     * 
     * @param request
     * @return
     * @throws RejectedExecutionException
     */
    ListenableFuture<O> submit(I request);
}
