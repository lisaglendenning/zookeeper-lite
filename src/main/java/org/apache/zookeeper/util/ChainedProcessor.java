package org.apache.zookeeper.util;

import java.util.List;

import com.google.common.collect.ForwardingList;
import com.google.common.collect.Lists;

public class ChainedProcessor<T> extends ForwardingList<Processor<T,T>> implements Processor<T,T> {
    
    public static <T> ChainedProcessor<T> create() {
        return new ChainedProcessor<T>();
    }

    protected final List<Processor<T,T>> processors;

    protected ChainedProcessor() {
        this(Lists.<Processor<T,T>>newArrayList());
    }

    protected ChainedProcessor(List<Processor<T,T>> processors) {
        this.processors = processors;
    }

    @Override
    protected List<Processor<T, T>> delegate() {
        return processors;
    }

    @Override
    public T apply(T input) throws Exception {
        for (Processor<T,T> processor: processors) {
            input = processor.apply(input);
        }
        return input;
    }

}
