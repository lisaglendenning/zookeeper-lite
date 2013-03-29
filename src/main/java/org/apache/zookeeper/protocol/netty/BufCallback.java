package org.apache.zookeeper.protocol.netty;

import io.netty.buffer.ByteBuf;

import com.google.common.util.concurrent.FutureCallback;

public class BufCallback implements FutureCallback<Void> {

    protected ByteBuf buf;
    
    public BufCallback(ByteBuf buf) {
        this.buf = buf;
        buf.retain();
    }
    
    @Override
    public void onSuccess(Void result) {
        buf.clear();
        buf.release();
    }

    @Override
    public void onFailure(Throwable t) {
        throw new AssertionError(t);
    }

}
