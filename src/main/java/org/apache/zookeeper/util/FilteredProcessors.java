package org.apache.zookeeper.util;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

public class FilteredProcessors<T,V> implements FilteringProcessor<T,V> {
    
    public static <T,V> FilteredProcessors<T,V> create(FilteringProcessor<T,V>...processors) {
        return new FilteredProcessors<T,V>(processors);
    }

    protected class Filter implements Predicate<T> {
        @Override
        public boolean apply(@Nullable T input) {
            for (FilteringProcessor<T,V> processor: processors) {
                if (processor.filter().apply(input)) {
                    return true;
                }
            }
            return false;
        }
        
    }

    protected final List<FilteringProcessor<T,V>> processors;
    protected final Filter filter;
    
    public FilteredProcessors(FilteringProcessor<T,V>...processors) {
        this.processors = Lists.newArrayList(processors);
        this.filter = new Filter();
    }
    
    @Override
    public V apply(T input) throws Exception {
        for (FilteringProcessor<T,V> processor: processors) {
            if (processor.filter().apply(input)) {
                return processor.apply(input);
            }
        }
        return null;
    }

    @Override
    public Predicate<? super T> filter() {
        return filter;
    }

}
