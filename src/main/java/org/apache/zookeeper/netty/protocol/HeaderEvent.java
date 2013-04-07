package org.apache.zookeeper.netty.protocol;

import com.google.common.util.concurrent.FutureCallback;

import io.netty.buffer.ByteBuf;

public class HeaderEvent extends BufEvent {

    protected ByteBuf header;

    public HeaderEvent() {
        super();
    }

    public HeaderEvent setHeader(ByteBuf header) {
        this.header = header;
        return this;
    }

    public ByteBuf getHeader() {
        return header;
    }

    @Override
    public HeaderEvent setBuf(ByteBuf buf) {
        super.setBuf(buf);
        return this;
    }

    @Override
    public HeaderEvent setCallback(FutureCallback<Void> callback) {
        super.setCallback(callback);
        return this;
    }
}
