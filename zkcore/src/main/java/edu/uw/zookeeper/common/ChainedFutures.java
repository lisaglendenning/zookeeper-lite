package edu.uw.zookeeper.common;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;

public final class ChainedFutures<V, T extends ListenableFuture<? extends V>> extends ToStringListenableFuture<V> implements Callable<Optional<List<T>>> {

    public static <U,T extends ListenableFuture<? extends U>,V> ChainedFuturesTask<U,T,V> run(
            ChainedFuturesProcessor<U,T,V> chain,
            Promise<V> promise) {
        ChainedFuturesTask<U,T,V> task = task(chain, promise);
        task.run();
        return task;
    }

    public static <U,T extends ListenableFuture<? extends U>,V> ChainedFuturesTask<U,T,V> task(
            ChainedFuturesProcessor<U,T,V> chain,
            Promise<V> promise) {
        return ChainedFuturesTask.create(chain, promise);
    }

    public static <V,T extends ListenableFuture<? extends V>> Processor<List<T>,V> getLast() {
        return new Processor<List<T>,V>() {
            @Override
            public V apply(List<T> input) throws Exception {
                return (V) input.get(input.size()-1).get();
            }
        };
    }
    
    public static <V> Processor<List<? extends ListenableFuture<?>>,V> castLast() {
        return new Processor<List<? extends ListenableFuture<?>>,V>() {
            @SuppressWarnings("unchecked")
            @Override
            public V apply(List<? extends ListenableFuture<?>> input) throws Exception {
                return (V) input.get(input.size()-1).get();
            }
        };
    }

    /**
     * Not threadsafe.
     */
    public static <U,T extends ListenableFuture<? extends U>,V> ChainedFuturesProcessor<U,T,V> process(
            ChainedFutures<U,T> chain,
            Processor<? super List<T>,? extends V> processor) {
        return ChainedFuturesProcessor.create(chain, processor);
    }

    /**
     * Not threadsafe.
     */
    public static <V, T extends ListenableFuture<? extends V>> ChainedFutures<V,T> chain(
            Function<? super List<T>, ? extends Optional<? extends T>> next,
            List<T> futures) {
        return chain(next, futures, LogManager.getLogger(ChainedFutures.class));
    }

    public static <V, T extends ListenableFuture<? extends V>> ChainedFutures<V,T> chain(
            Function<? super List<T>, ? extends Optional<? extends T>> next,
            List<T> futures,
            Logger logger) {
        return new ChainedFutures<V,T>(next, futures, logger);
    }
    
    private final Logger logger;
    private final Function<? super List<T>, ? extends Optional<? extends T>> next;
    private final List<T> futures;
    
    protected ChainedFutures(
            Function<? super List<T>, ? extends Optional<? extends T>> next,
            List<T> futures,
            Logger logger) {
        this.logger = logger;
        this.next = next;
        this.futures = futures;
    }
    
    public List<T> futures() {
        return futures;
    }

    @Override
    public Optional<List<T>> call() throws Exception {
        if (futures.isEmpty() || isDone()) {
            Optional<? extends T> next = this.next.apply(futures);
            logger.trace("APPLIED {} TO {} => {}", this.next, futures, next.orNull());
            if (next.isPresent()) {
                futures.add(next.get());
            } else {
                return Optional.of(futures);
            }
        }
        return Optional.absent();
    }
    
    @Override
    protected Objects.ToStringHelper toStringHelper(Objects.ToStringHelper helper) {
        helper.addValue(next);
        ImmutableList.Builder<String> values = ImmutableList.builder();
        for (T future: futures) {
            values.add(ToStringListenableFuture.toString3rdParty(future));
        }
        return helper.addValue(values.build());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ListenableFuture<V> delegate() {
        return (ListenableFuture<V>) (ListenableFuture<?>) futures.get(futures.size() - 1);
    }

    public static final class ChainedFuturesProcessor<U,T extends ListenableFuture<? extends U>,V> extends ForwardingListenableFuture<U> implements Callable<Optional<V>> {

        public static <U,T extends ListenableFuture<? extends U>,V> ChainedFuturesProcessor<U,T,V> create(
                ChainedFutures<U,T> chain,
                Processor<? super List<T>,? extends V> processor) {
            return new ChainedFuturesProcessor<U,T,V>(chain, processor);
        }
        
        private final ChainedFutures<U,T> chain;
        private final Processor<? super List<T>,? extends V> processor;
        
        protected ChainedFuturesProcessor(
                ChainedFutures<U, T> chain,
                Processor<? super List<T>,? extends V> processor) {
            super();
            this.chain = chain;
            this.processor = processor;
        }

        @Override
        public Optional<V> call() throws Exception {
            Optional<List<T>> futures = chain.call();
            if (futures.isPresent()) {
                return Optional.<V>of(processor.apply(futures.get()));
            }
            return Optional.absent();
        }
        
        @Override
        public ChainedFutures<U,T> delegate() {
            return chain;
        }
    }
    
    public static final class ChainedFuturesTask<U,T extends ListenableFuture<? extends U>,V> extends CallablePromiseTask<ChainedFuturesProcessor<U,T,V>,V> implements Runnable {

        public static <U,T extends ListenableFuture<? extends U>,V> ChainedFuturesTask<U,T,V> create(
                ChainedFuturesProcessor<U,T,V> chain,
                Promise<V> promise) {
            return new ChainedFuturesTask<U,T,V>(chain, promise);
        }
        
        protected ChainedFuturesTask(
                ChainedFuturesProcessor<U,T,V> chain,
                Promise<V> promise) {
            super(chain, promise);
        }

        @Override
        public synchronized void run() {
            super.run();
            if (!isDone()) {
                task().addListener(this, SameThreadExecutor.getInstance());
            } else if (isCancelled()) {
                if (!task().delegate().futures().isEmpty()) {
                    task().delegate().cancel(false);
                }
            }
        }
        
        @Override
        public synchronized String toString() {
            return super.toString();
        }
    }
}
