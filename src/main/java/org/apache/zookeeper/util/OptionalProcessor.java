package org.apache.zookeeper.util;

import com.google.common.base.Predicate;

public class OptionalProcessor<T> implements FilteringProcessor<T,T> {

    public static <T> OptionalProcessor<T> create(FilteringProcessor<T, T> processor) {
        return new OptionalProcessor<T>(processor);
    }
    
    protected final FilteringProcessor<T, T> processor;
    
    public OptionalProcessor(FilteringProcessor<T, T> processor) {
        this.processor = processor;
    }

    @Override
    public T apply(T input) throws Exception {
        if (processor.filter().apply(input)) {
            return processor.apply(input);
        }
        return input;
    }

    @Override
    public Predicate<? super T> filter() {
        return processor.filter();
    }
}
