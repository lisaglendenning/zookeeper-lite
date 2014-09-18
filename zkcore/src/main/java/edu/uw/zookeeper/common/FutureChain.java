package edu.uw.zookeeper.common;

import java.util.Collection;
import java.util.Deque;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;

public interface FutureChain<E extends ListenableFuture<?>> extends Collection<E>, Iterable<E> {
    E getLast();
    
    public interface FutureListChain<E extends ListenableFuture<?>> extends FutureChain<E>, List<E> {}
    
    public interface FutureDequeChain<E extends ListenableFuture<?>> extends FutureChain<E>, Deque<E> {}
}