package edu.uw.zookeeper.util;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;
import com.google.common.collect.ForwardingQueue;
import com.google.common.util.concurrent.ListenableFuture;

public class TaskMailbox<V, T extends Future<? extends V>> extends ForwardingQueue<T> {

    public static <I,O> PromiseTaskProcessor<I,O> processor(
            Processor<I,O> delegate) {
        return PromiseTaskProcessor.newInstance(delegate);
    }
    
    public static <I,O,V> AbstractActor.SimpleActor<PromiseTask<I,V>, O> actor(
            Processor<PromiseTask<I,V>, O> processor,
            Executor executor) {
        TaskMailbox<V, PromiseTask<I,V>> mailbox = newQueue();
        return AbstractActor.newInstance(
                processor, 
                executor, 
                mailbox, 
                new AtomicReference<Actor.State>(Actor.State.WAITING));
    }
    
    public static <I,V> ActorTaskExecutor<I,V> executor(
            Actor<PromiseTask<I,V>> actor) {
        return ActorTaskExecutor.newInstance(actor);
    }
        
    public static class PromiseTaskProcessor<I,O> implements Processor<PromiseTask<I,O>, O> {

        public static <I,O> PromiseTaskProcessor<I,O> newInstance(Processor<I,O> delegate) {
            return new PromiseTaskProcessor<I,O>(delegate);
        }
        
        protected final Processor<I,O> delegate;
        
        protected PromiseTaskProcessor(Processor<I,O> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public O apply(PromiseTask<I,O> input) throws Exception {
            O output = null;
            if (! input.isDone()) {
                try {
                    output = delegate.apply(input.task());
                } catch (Throwable e) {
                    if (! input.isDone()) {
                        input.setException(e);
                    }
                    throw Throwables.propagate(e);
                }
                input.set(output);
            }
            return output;
        }
    }
    
    public static class ActorTaskExecutor<I,V> implements TaskExecutor<I,V>, Reference<Actor<PromiseTask<I,V>>> {

        public static <I,V> ActorTaskExecutor<I,V> newInstance(Actor<PromiseTask<I,V>> actor) {
            return new ActorTaskExecutor<I,V>(actor);
        }
        
        protected final Actor<PromiseTask<I,V>> actor;
        
        protected ActorTaskExecutor(Actor<PromiseTask<I,V>> actor) {
            this.actor = actor;
        }
        
        @Override
        public Actor<PromiseTask<I,V>> get() {
            return actor;
        }

        @Override
        public ListenableFuture<V> submit (I request) {
            return submit(request, newPromise());
        }

        @Override
        public ListenableFuture<V> submit(I request, Promise<V> promise) {
            PromiseTask<I,V> task = newPromiseTask(request, promise);
            try {
                get().send(task);
            } catch (RejectedExecutionException e) {
                task.cancel(true);
                throw e;
            }
            return task;
        }
        
        public Promise<V> newPromise() {
            return PromiseTask.newPromise();
        }

        protected PromiseTask<I,V> newPromiseTask(I request, Promise<V> promise) {
            return PromiseTask.of(request, promise);
        }
    }

    public static <V, T extends Future<? extends V>> TaskMailbox<V,T> newQueue() {
        return newInstance(AbstractActor.<T>newQueue());
    }
    
    public static <V, T extends Future<? extends V>> TaskMailbox<V,T> newInstance(Queue<T> delegate) {
        return new TaskMailbox<V,T>(delegate);
    }
    
    protected final Queue<T> delegate;
    
    protected TaskMailbox(Queue<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void clear() {
        T next;
        while ((next = poll()) != null) {
            if (! next.isDone()) {
                next.cancel(true);
            }
        }
    }

    @Override
    protected Queue<T> delegate() {
        return delegate;
    }
}
