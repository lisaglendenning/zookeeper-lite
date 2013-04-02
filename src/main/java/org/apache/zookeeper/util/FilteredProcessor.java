package org.apache.zookeeper.util;

import com.google.common.base.Predicate;

public class FilteredProcessor<T,V> extends Pair<Predicate<? super T>, Processor<T,V>> implements FilteringProcessor<T,V> {

    public static <T,V> FilteredProcessor<T,V> create(Predicate<? super T> first, Processor<T, V> second) {
        return new FilteredProcessor<T,V>(first, second);
    }
    
    public FilteredProcessor(Predicate<? super T> first, Processor<T, V> second) {
        super(first, second);
    }

    @Override
    public V apply(T input) throws Exception {
        if (filter().apply(input)) {
            return second().apply(input);
        }
        return null;
    }

    @Override
    public Predicate<? super T> filter() {
        return first();
    }

}
