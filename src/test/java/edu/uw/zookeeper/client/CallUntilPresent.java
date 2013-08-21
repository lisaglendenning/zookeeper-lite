package edu.uw.zookeeper.client;

import java.util.concurrent.Callable;

import com.google.common.base.Optional;

public class CallUntilPresent<O> implements Callable<O> {

    public static <O> CallUntilPresent<O> create(Callable<Optional<O>> callable) {
        return new CallUntilPresent<O>(callable);
    }
    
    protected final Callable<Optional<O>> callable;
    
    public CallUntilPresent(Callable<Optional<O>> callable) {
        this.callable = callable;
    }
    
    @Override
    public O call() throws Exception {
        Optional<O> result;
        do {
            result = callable.call();
        } while (!result.isPresent());
        return result.get();
    }
}
