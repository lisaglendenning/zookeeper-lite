package edu.uw.zookeeper.common;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;

public abstract class ToStringListenableFuture<V> extends ForwardingListenableFuture<V> {

    public static <V> SimpleToStringListenableFuture<V> simple(
            ListenableFuture<V> delegate) {
        return new SimpleToStringListenableFuture<V>(delegate);
    }
    
    public static String toString(Future<?> future) {
        Object value;
        if (future.isDone()) {
            if (future.isCancelled()) {
                value = "cancelled";
            } else {
                try {
                    value = future.get();
                } catch (InterruptedException e) {
                    value = e;
                } catch (ExecutionException e) {
                    value = e.getCause();
                }
            }
        } else {
            value = "";
        }
        return String.valueOf(value);
    }
    
    public static boolean is3rdParty(Object obj) {
        return !obj.getClass().getName().startsWith("edu.uw.");
    }
    
    public static String toString3rdParty(Future<?> future) {
        return is3rdParty(future) ? toString(future) : future.toString();
    }
    
    public static Function<Future<?>, String> toString3rdParty() {
        return new Function<Future<?>, String>() {
            @Override
            public String apply(Future<?> input) {
                return toString3rdParty(input);
            }
        };
    }

    protected ToStringListenableFuture() {
        super();
    }
    
    @Override
    public String toString() {
        return toStringHelper().toString();
    }
    
    protected MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(MoreObjects.toStringHelper(this));
    }
    
    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
        return helper.addValue(toString(this));
    }
    
    public static class SimpleToStringListenableFuture<V> extends ToStringListenableFuture<V> {

        private final ListenableFuture<V> delegate;

        protected SimpleToStringListenableFuture(
                ListenableFuture<V> delegate) {
            this.delegate = Preconditions.checkNotNull(delegate);
        }

        @Override
        protected final ListenableFuture<V> delegate() {
          return delegate;
        }
    }
}
