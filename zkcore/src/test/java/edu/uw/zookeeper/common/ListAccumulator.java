package edu.uw.zookeeper.common;

import java.util.List;
import java.util.concurrent.Callable;

public class ListAccumulator<V> implements Callable<List<V>> {
    
    public static <V> ListAccumulator<V> create(Callable<V> callable, List<V> result) {
        return new ListAccumulator<V>(callable, result);
    }
    
    private final Callable<V> callable;
    private final List<V> result;
    
    public ListAccumulator(Callable<V> callable, List<V> result) {
        this.callable = callable;
        this.result = result;
    }

    @Override
    public List<V> call()
            throws Exception {
        result.add(callable.call());
        return result;
    }
}