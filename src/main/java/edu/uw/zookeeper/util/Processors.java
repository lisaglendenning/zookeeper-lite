package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Predicate;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.Lists;

public abstract class Processors {
    
    public static interface UncheckedProcessor<V,T> extends Processor<V,T> {
        @Override
        public T apply(V input);
    }

    public static interface CheckedProcessor<V,T,E extends Exception> extends Processor<V,T> {
        @Override
        public T apply(V input) throws E;
    }
    
    public static <T> Identity<T> identity() {
        return new Identity<T>();
    }
    
    public static class Identity<T> implements Processor<T, T> {
        public Identity() {}

        @Override
        public T apply(T input) {
            return input;
        }
    }
    
    public static abstract class ForwardingProcessor<V,T> implements Processor<V,T> {
        
        protected abstract Processor<? super V, ? extends T> delegate();

        @Override
        public T apply(V input) throws Exception {
            return delegate().apply(input);
        }
    }
    
    public static <V,U,T> ProcessorBridge<V,U,T> bridge(
            Processor<? super V, ? extends U> first, Processor<? super U, ? extends T> second) {
        return ProcessorBridge.newInstance(first, second);
    }
    
    public static class ProcessorBridge<V,U,T> extends
        Pair<Processor<? super V, ? extends U>, Processor<? super U, ? extends T>> implements Processor<V,T> {
    
        public static <V,U,T> ProcessorBridge<V,U,T> newInstance(
                Processor<? super V, ? extends U> first, Processor<? super U, ? extends T> second) {
            return new ProcessorBridge<V,U,T>(first, second);
        }
        
        public ProcessorBridge(
                Processor<? super V, ? extends U> first, Processor<? super U, ? extends T> second) {
            super(checkNotNull(first), checkNotNull(second));
        }
        
        @Override
        public T apply(V input) throws Exception {
            return second().apply(first().apply(input));
        }
    }
    
    public static <T> ProcessorChain<T> chain(List<Processor<T,T>> delegate) {
        return ProcessorChain.newInstance(delegate);
    }

    public static class ProcessorChain<T> extends
            ForwardingList<Processor<T,T>> implements Processor<T,T> {

        public static <T> ProcessorChain<T> newInstance() {
            return new ProcessorChain<T>();
        }

        public static <T> ProcessorChain<T> newInstance(List<Processor<T,T>> delegate) {
            return new ProcessorChain<T>(delegate);
        }

        private final List<Processor<T,T>> delegate;

        public ProcessorChain() {
            this(Lists.<Processor<T,T>> newArrayList());
        }

        public ProcessorChain(List<Processor<T,T>> delegate) {
            this.delegate = delegate;
        }

        @Override
        protected List<Processor<T,T>> delegate() {
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

    public static class ProcessorThunk<V,T> extends AbstractPair<Processor<? super V, ? extends T>, V> implements Callable<T> {

        public static <V,T> ProcessorThunk<V,T> newInstance(
                Processor<? super V, ? extends T> first, V second) {
            return new ProcessorThunk<V,T>(first, second);
        }
        
        public ProcessorThunk(
                Processor<? super V, ? extends T> first, V second) {
            super(first, second);
        }

        @Override
        public T call() throws Exception {
            return first.apply(second);
        }
    }
    
    private Processors() {}
}
