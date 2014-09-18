package edu.uw.zookeeper.common;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ForwardingCollection;
import com.google.common.collect.ForwardingDeque;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Not threadsafe.
 */
public class ChainedFutures<E extends ListenableFuture<?>, V extends FutureChain<E>, U extends ChainedFutures.ChainedProcessor<E,? super V>> extends ForwardingCollection<E> implements FutureChain<E>, Callable<Optional<V>> {

    public static interface ChainedProcessor<E extends ListenableFuture<?>, T extends FutureChain<E>> extends Processor<T, Optional<? extends E>> {
    }

    public static <V> ChainedFuturesTask<V> run(
            ChainedResult<V,?,?,?> chain) {
        return run(chain, SettableFuturePromise.<V>create());
    }

    public static <V> ChainedFuturesTask<V> run(
            ChainedResult<V,?,?,?> chain,
            Promise<V> promise) {
        ChainedFuturesTask<V> task = task(chain, promise);
        task.run();
        return task;
    }

    public static <V> ChainedFuturesTask<V> task(
            ChainedResult<V,?,?,?> chain) {
        return task(chain, SettableFuturePromise.<V>create());
    }

    public static <V> ChainedFuturesTask<V> task(
            ChainedResult<V,?,?,?> chain,
            Promise<V> promise) {
        return new ChainedFuturesTask<V>(chain, promise);
    }
    
    public static <V, C extends FutureChain<?>, T extends Processor<? super C, ? extends V>, U extends ChainedFutures<?,? extends C,?>> ChainedResult<V,C,T,U> result(
            T result, U chain) {
        return ChainedResult.create(result, chain);
    }

    public static <V> Processor<FutureChain<?>,V> castLast() {
        return new Processor<FutureChain<?>,V>() {
            @SuppressWarnings("unchecked")
            @Override
            public V apply(FutureChain<?> input) throws Exception {
                return (V) input.getLast().get();
            }
        };
    }

    public static <V> ChainedResult<V,?,?,?> castLast(ChainedFutures<?,?,?> chain) {
        return result(ChainedFutures.<V>castLast(), chain);
    }
    
    public static <E extends ListenableFuture<?>, V extends FutureChain<E>, U extends ChainedFutures.ChainedProcessor<E,? super V>> ChainedFutures<E,V,U> apply(
            U processor,
            V chain) {
        return apply(processor, chain, LogManager.getLogger(ChainedFutures.class));
    }
    
    public static <E extends ListenableFuture<?>, U extends ChainedFutures.ChainedProcessor<E,? super ListChain<E,LinkedList<E>>>> ChainedFutures<E,ListChain<E,LinkedList<E>>,U> linkedList(
            U processor) {
        return list(processor, Lists.<E>newLinkedList());
    }
    
    public static <E extends ListenableFuture<?>, U extends ChainedFutures.ChainedProcessor<E,? super ListChain<E,ArrayList<E>>>> ChainedFutures<E,ListChain<E,ArrayList<E>>,U> arrayList(
            U processor) {
        return list(processor, Lists.<E>newArrayList());
    }
    
    public static <E extends ListenableFuture<?>, U extends ChainedFutures.ChainedProcessor<E,? super ListChain<E,ArrayList<E>>>> ChainedFutures<E,ListChain<E,ArrayList<E>>,U> arrayList(
            U processor,
            int capacity) {
        return list(processor, Lists.<E>newArrayListWithCapacity(capacity));
    }
    
    public static <E extends ListenableFuture<?>, T extends List<E>, U extends ChainedFutures.ChainedProcessor<E,? super ListChain<E,T>>> ChainedFutures<E,ListChain<E,T>,U> list(
            U processor,
            T chain) {
        return apply(processor, list(chain));
    }
    
    public static <E extends ListenableFuture<?>, U extends ChainedFutures.ChainedProcessor<E,? super DequeChain<E,ArrayDeque<E>>>> ChainedFutures<E,DequeChain<E,ArrayDeque<E>>,U> arrayDeque(
            U processor) {
        return deque(processor, Queues.<E>newArrayDeque());
    }
    
    public static <E extends ListenableFuture<?>, T extends Deque<E>, U extends ChainedFutures.ChainedProcessor<E,? super DequeChain<E,T>>> ChainedFutures<E,DequeChain<E,T>,U> deque(
            U processor,
            T chain) {
        return apply(processor, deque(chain));
    }

    public static <E extends ListenableFuture<?>, V extends FutureChain<E>, U extends ChainedFutures.ChainedProcessor<E,? super V>> ChainedFutures<E,V,U> apply(
            U processor,
            V chain,
            Logger logger) {
        return new ChainedFutures<E,V,U>(processor, chain, logger);
    }
    
    public static <E extends ListenableFuture<?>, T extends List<E>> ListChain<E,T> list(T delegate) {
        return ListChain.create(delegate);
    }

    public static <E extends ListenableFuture<?>, T extends Deque<E>> DequeChain<E,T> deque(T delegate) {
        return new DequeChain<E,T>(delegate);
    }
    
    protected final Logger logger;
    protected final U processor;
    protected final V chain;
    
    protected ChainedFutures(
            U processor,
            V chain,
            Logger logger) {
        this.logger = logger;
        this.processor = processor;
        this.chain = chain;
    }
    
    public U processor() {
        return processor;
    }
    
    public V chain() {
        return chain;
    }

    @Override
    public Optional<V> call() throws Exception {
        if (chain().isEmpty() || chain().getLast().isDone()) {
            Optional<? extends E> next = processor().apply(chain());
            if (logger.isDebugEnabled()) {
                logger.debug("APPLIED {} TO {} => {}", 
                        processor(), 
                        Collections2.transform(chain(), ToStringListenableFuture.toString3rdParty()), 
                        next.orNull());
            }
            if (next.isPresent()) {
                if (!chain().add(LoggingFutureListener.listen(logger, next.get()))) {
                    throw new IllegalStateException();
                }
            } else {
                return Optional.of(chain());
            }
        }
        return Optional.absent();
    }
    
    @Override
    public E getLast() {
        return chain().getLast();
    }

    @Override
    public String toString() {
        return toStringHelper(MoreObjects.toStringHelper(this)).toString();
    }
    
    @Override
    protected Collection<E> delegate() {
        return chain();
    }

    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
        helper.addValue(processor());
        ImmutableList.Builder<String> values = ImmutableList.builder();
        for (E future: chain()) {
            values.add(ToStringListenableFuture.toString3rdParty(future));
        }
        return helper.addValue(values.build());
    }
    
    public static class ListChain<E extends ListenableFuture<?>, T extends List<E>> extends ForwardingList<E> implements FutureChain.FutureListChain<E> {

        public static <E extends ListenableFuture<?>, T extends List<E>> ListChain<E,T> create(T delegate) {
            return new ListChain<E,T>(delegate);
        }
        
        private final T delegate;
        
        protected ListChain(
                T delegate) {
            this.delegate = delegate;
        }

        @Override
        public E getLast() {
            return get(size() - 1);
        }

        @Override
        protected T delegate() {
            return delegate;
        }
    }
    
    public static class DequeChain<E extends ListenableFuture<?>, T extends Deque<E>> extends ForwardingDeque<E> implements FutureChain.FutureDequeChain<E> {

        public static <E extends ListenableFuture<?>, T extends Deque<E>> DequeChain<E,T> create(T delegate) {
            return new DequeChain<E,T>(delegate);
        }

        private final T delegate;
        
        protected DequeChain(
                T delegate) {
            this.delegate = delegate;
        }

        @Override
        protected T delegate() {
            return delegate;
        }
    }

    /**
     * Not threadsafe.
     */
    public static final class ChainedResult<V, C extends FutureChain<?>, T extends Processor<? super C, ? extends V>, U extends ChainedFutures<?,? extends C,?>> extends AbstractPair<T,U> implements Callable<Optional<V>> {

        public static <V, C extends FutureChain<?>, T extends Processor<? super C, ? extends V>, U extends ChainedFutures<?,? extends C,?>> ChainedResult<V,C,T,U> create(
                T result, U chain) {
            return new ChainedResult<V,C,T,U>(result, chain);
        }
        
        protected ChainedResult(
                T result, U chain) {
            super(result, chain);
        }
        
        public T result() {
            return first;
        }
        
        public U chain() {
            return second;
        }

        @Override
        public Optional<V> call() throws Exception {
            Optional<? extends C> futures = chain().call();
            if (futures.isPresent()) {
                return Optional.<V>of(result().apply(futures.get()));
            }
            return Optional.absent();
        }
    }
    
    public static final class ChainedFuturesTask<V> extends CallablePromiseTask<ChainedResult<V,?,?,?>,V> implements Runnable {

        public static <V> ChainedFuturesTask<V> create(
                ChainedResult<V,?,?,?> chain,
                Promise<V> promise) {
            return new ChainedFuturesTask<V>(chain, promise);
        }
        
        protected ChainedFuturesTask(
                ChainedResult<V,?,?,?> chain,
                Promise<V> promise) {
            super(chain, promise);
        }

        @Override
        public synchronized void run() {
            FutureChain<?> chain = task().chain().chain();
            if (!isDone()) {
                super.run();
                chain.getLast().addListener(this, MoreExecutors.directExecutor());
            } else if (isCancelled()) {
                if (!chain.isEmpty()) {
                    chain.getLast().cancel(false);
                }
            }
        }
        
        @Override
        public synchronized String toString() {
            return super.toString();
        }
    }
}
