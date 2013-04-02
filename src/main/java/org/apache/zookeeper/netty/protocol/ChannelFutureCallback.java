package org.apache.zookeeper.netty.protocol;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import com.google.common.util.concurrent.FutureCallback;

public class ChannelFutureCallback implements ChannelFutureListener {

    protected FutureCallback<Void> callback;
    
    public ChannelFutureCallback(FutureCallback<Void> callback) {
        this.callback = callback;
    }
    
    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
            callback.onSuccess(null);
        } else {
            callback.onFailure(future.cause());
        }
    }
}