package edu.uw.zookeeper.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import com.google.common.util.concurrent.SettableFuture;

public class ChannelFutureWrapper<T> implements ChannelFutureListener {

    public static <T> ChannelFutureWrapper<T> create(ChannelFuture future,
            T result) {
        return new ChannelFutureWrapper<T>(future, result);
    }

    protected final T result;
    protected final SettableFuture<T> promise;

    public ChannelFutureWrapper(ChannelFuture future, T result) {
        this.result = result;
        this.promise = SettableFuture.create();
        future.addListener(this);
    }

    public SettableFuture<T> promise() {
        return promise;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        assert (future.isDone());
        if (future.isSuccess()) {
            promise.set(result);
        } else {
            if (future.isCancelled()) {
                promise.cancel(true);
            } else {
                promise.setException(future.cause());
            }
        }
    }
}
