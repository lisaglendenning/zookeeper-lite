package edu.uw.zookeeper.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;

public class ChannelFutureWrapper<V> extends PromiseTask<ChannelFuture, V> implements ChannelFutureListener {

    public static <V> ChannelFutureWrapper<V> of(
            ChannelFuture future,
            V result) {
        Promise<V> promise = newPromise();
        return of(future, result, promise);
    }

    public static <V> ChannelFutureWrapper<V> of(
            ChannelFuture future,
            V result,
            Promise<V> promise) {
        ChannelFutureWrapper<V> wrapper = new ChannelFutureWrapper<V>(future, result, promise);
        future.addListener(wrapper);
        return wrapper;
    }

    protected final V result;
    
    protected ChannelFutureWrapper(ChannelFuture future, V result, Promise<V> promise) {
        super(future, promise);
        this.result = result;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (task().cancel(mayInterruptIfRunning)) {
            return super.cancel(mayInterruptIfRunning);
        }
        return false;
    }
    
    @Override
    public void operationComplete(ChannelFuture future) {
        assert (future.isDone());
        if (future.isSuccess()) {
            set(result);
        } else {
            if (future.isCancelled()) {
                cancel(true);
            } else {
                setException(future.cause());
            }
        }
    }
}
