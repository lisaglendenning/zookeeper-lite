package org.apache.zookeeper.protocol;

import java.util.List;

import com.google.common.collect.Lists;

public class Pipeline<T> {
    public static interface Processor<T> {
        T apply(T input);
    }
    
    protected final List<Processor<T>> delegate;
    
    public Pipeline() {
        this.delegate = Lists.newArrayList();
    }
    
    public void add(Processor<T> processor) {
        this.delegate.add(processor);
    }
    
    @SuppressWarnings({ })
    public T apply(T input) {
        for (Processor<T> processor: delegate) {
            input = processor.apply(input);
        }
        return input;
    }
}
