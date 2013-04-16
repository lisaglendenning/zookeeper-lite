package edu.uw.zookeeper.util;

import java.util.List;

import com.google.common.collect.ForwardingList;
import com.google.common.collect.Lists;

public class ProcessorChain<T> extends ForwardingList<Processor<T, T>>
        implements Processor<T, T> {

    public static <T> ProcessorChain<T> create() {
        return new ProcessorChain<T>();
    }

    private final List<Processor<T, T>> delegate;

    protected ProcessorChain() {
        this(Lists.<Processor<T, T>> newArrayList());
    }

    protected ProcessorChain(List<Processor<T, T>> delegate) {
        this.delegate = delegate;
    }

    @Override
    protected List<Processor<T, T>> delegate() {
        return delegate;
    }

    @Override
    public T apply(T input) throws Exception {
        for (Processor<T, T> processor : this) {
            input = processor.apply(input);
        }
        return input;
    }

}
