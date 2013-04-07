package org.apache.zookeeper.netty.protocol;

import com.google.common.util.concurrent.FutureCallback;

import io.netty.buffer.ByteBuf;

public class BufEvent extends CallbackEvent {
    protected ByteBuf buf = null;

    public BufEvent setBuf(ByteBuf buf) {
        this.buf = buf;
        return this;
    }

    public ByteBuf getBuf() {
        return buf;
    }

    @Override
    public BufEvent setCallback(FutureCallback<Void> callback) {
        super.setCallback(callback);
        return this;
    }
}
