package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.Lists;

public abstract class Processors {
    public static class ProcessorBridge<T, U, V> extends
        Pair<Processor<T, U>, Processor<U, V>> implements Processor<T, V> {
    
        public static <T, U, V> ProcessorBridge<T, U, V> newInstance(
                Processor<T, U> first, Processor<U, V> second) {
            return new ProcessorBridge<T, U, V>(first, second);
        }
        
        protected ProcessorBridge(Processor<T, U> first, Processor<U, V> second) {
            super(checkNotNull(first), checkNotNull(second));
        }
        
        @Override
        public V apply(T input) throws Exception {
            return second().apply(first().apply(input));
        }
    }

    public static class ProcessorChain<T> extends
            ForwardingList<Processor<T, T>> implements Processor<T, T> {

        public static <T> ProcessorChain<T> newInstance() {
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

    public static interface FilteringProcessor<V,T> extends Processor<V,T> {
        Predicate<? super V> filter();
    }

    public static class FilteredProcessor<V,T> extends
            Pair<Predicate<? super V>, Processor<? super V, ? extends T>> implements
            FilteringProcessor<V,T> {

        public static <V,T> FilteredProcessor<V,T> newInstance(
                Predicate<? super V> first, Processor<? super V, ? extends T> second) {
            return new FilteredProcessor<V,T>(first, second);
        }

        public FilteredProcessor(Predicate<? super V> first,
                Processor<? super V, ? extends T> second) {
            super(first, second);
        }

        @Override
        public T apply(V input) throws Exception {
            if (filter().apply(input)) {
                return second().apply(input);
            }
            return null;
        }

        @Override
        public Predicate<? super V> filter() {
            return first();
        }

    }

    public static class FilteredProcessors<V,T> implements
            FilteringProcessor<V,T> {

        public static <V,T> FilteredProcessors<V,T> newInstance(
                FilteringProcessor<? super V, ? extends T>... processors) {
            return new FilteredProcessors<V,T>(processors);
        }

        private class Filter implements Predicate<V> {
            @Override
            public boolean apply(V input) {
                for (FilteringProcessor<? super V, ? extends T> processor : processors) {
                    if (processor.filter().apply(input)) {
                        return true;
                    }
                }
                return false;
            }
        }

        private final List<FilteringProcessor<? super V, ? extends T>> processors;
        private final Filter filter;

        public FilteredProcessors(FilteringProcessor<? super V, ? extends T>... processors) {
            this.processors = Lists.newArrayList(processors);
            this.filter = new Filter();
        }

        @Override
        public T apply(V input) throws Exception {
            for (FilteringProcessor<? super V, ? extends T> processor : processors) {
                if (processor.filter().apply(input)) {
                    return processor.apply(input);
                }
            }
            return null;
        }

        @Override
        public Predicate<? super V> filter() {
            return filter;
        }
    }

    public static class OptionalProcessor<T> implements
            FilteringProcessor<T, T> {

        public static <T> OptionalProcessor<T> newInstance(
                FilteringProcessor<T, T> processor) {
            return new OptionalProcessor<T>(processor);
        }

        private final FilteringProcessor<T, T> processor;

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

}
