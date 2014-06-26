package edu.uw.zookeeper.common;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;

public class ToStringListenableFuture<V> extends ForwardingListenableFuture.SimpleForwardingListenableFuture<V> {

    public static <V> ToStringListenableFuture<V> create(
            ListenableFuture<V> delegate) {
        return new ToStringListenableFuture<V>(delegate);
    }

    public static String toString(Future<?> future) {
        return toString(future, Objects.toStringHelper(future));
    }

    public static String toString(Future<?> future, Objects.ToStringHelper toString) {
        if (future.isDone()) {
            Object value;
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
            toString.addValue(value);
        }
        return toString.toString();
    }

    protected ToStringListenableFuture(
            ListenableFuture<V> delegate) {
        super(delegate);
    }
    
    @Override
    public String toString() {
        return toString(this);
    }
}
