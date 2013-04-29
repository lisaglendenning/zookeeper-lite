package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.Lists;

public abstract class Processors {
    public static class ProcessorBridge<T, U, V> extends
        Pair<Processor<T, U>, Processor<U, V>> implements Processor<T, V> {
    
        public static <T, U, V> ProcessorBridge<T, U, V> create(
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

    public static interface FilteringProcessor<T, V> extends Processor<T, V> {
        Predicate<? super T> filter();
    }

    public static class FilteredProcessor<T, V> extends
            Pair<Predicate<? super T>, Processor<T, V>> implements
            FilteringProcessor<T, V> {

        public static <T, V> FilteredProcessor<T, V> create(
                Predicate<? super T> first, Processor<T, V> second) {
            return new FilteredProcessor<T, V>(first, second);
        }

        public FilteredProcessor(Predicate<? super T> first,
                Processor<T, V> second) {
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

    public static class FilteredProcessors<T, V> implements
            FilteringProcessor<T, V> {

        public static <T, V> FilteredProcessors<T, V> create(
                FilteringProcessor<T, V>... processors) {
            return new FilteredProcessors<T, V>(processors);
        }

        private class Filter implements Predicate<T> {
            @Override
            public boolean apply(@Nullable T input) {
                for (FilteringProcessor<T, V> processor : processors) {
                    if (processor.filter().apply(input)) {
                        return true;
                    }
                }
                return false;
            }
        }

        private final List<FilteringProcessor<T, V>> processors;
        private final Filter filter;

        public FilteredProcessors(FilteringProcessor<T, V>... processors) {
            this.processors = Lists.newArrayList(processors);
            this.filter = new Filter();
        }

        @Override
        public V apply(T input) throws Exception {
            for (FilteringProcessor<T, V> processor : processors) {
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

    public static class OptionalProcessor<T> implements
            FilteringProcessor<T, T> {

        public static <T> OptionalProcessor<T> create(
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
