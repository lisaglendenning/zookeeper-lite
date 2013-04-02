package org.apache.zookeeper.netty.protocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import com.google.common.base.Function;
import com.google.common.util.concurrent.SettableFuture;

public class ListenableChannelFuture<T> implements ChannelFutureListener {

    protected final SettableFuture<T> wrapper;
    protected final Function<Channel, T> toResult;
    
    public ListenableChannelFuture(ChannelFuture future, Function<Channel, T> toResult) {
        this.toResult = toResult;
        this.wrapper = SettableFuture.create();
        future.addListener(this);
    }
    
    public SettableFuture<T> getWrapper() {
        return wrapper;
    }
    
    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
        assert (future.isDone());
        if (future.isSuccess()) {
            wrapper.set(toResult.apply(future.channel()));
        } else {
            getWrapper().setException(future.cause());
        }
    }
}