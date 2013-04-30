package edu.uw.zookeeper.server;

import edu.uw.zookeeper.util.Processor;

public class ErrorProcessor<V,T> implements Processor<V,T> {

    public static <V,T> ErrorProcessor<V,T> create() {
        return new ErrorProcessor<V,T>();
    }

    public ErrorProcessor() {
    }

    @Override
    public T apply(V input) {
        throw new IllegalArgumentException(input.toString());
    }
}
