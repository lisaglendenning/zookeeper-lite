package edu.uw.zookeeper.server;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.TaskExecutor;

public class ProcessorTaskExecutor<I,O> implements TaskExecutor<I,O> {

    public static <I,O> ProcessorTaskExecutor<I,O> of(
            Processor<? super I, ? extends O> processor) {
        return new ProcessorTaskExecutor<I,O>(processor);
    }
    
    protected final Processor<? super I, ? extends O> processor;
    
    public ProcessorTaskExecutor(Processor<? super I, ? extends O> processor) {
        this.processor = processor;
    }
    
    @Override
    public ListenableFuture<O> submit(I request) {
        try {
            return Futures.immediateFuture((O) processor.apply(request));
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }
}