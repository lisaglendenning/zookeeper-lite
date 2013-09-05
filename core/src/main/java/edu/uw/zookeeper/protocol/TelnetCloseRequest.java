package edu.uw.zookeeper.protocol;

import com.google.common.base.Optional;

import io.netty.buffer.ByteBuf;

public enum TelnetCloseRequest implements Message.ClientAnonymous {
    TELNET_CLOSE_REQUEST;
    
    public static Optional<TelnetCloseRequest> decode(ByteBuf out) {
        if (out.readableBytes() >= LENGTH) {
            int value = out.getInt(out.readerIndex());
            TelnetCloseRequest instance = getInstance();
            if (value == instance.intValue()) {
                out.skipBytes(LENGTH);
                return Optional.of(instance);
            }
        }
        return Optional.absent();
    }
    
    public static TelnetCloseRequest getInstance() {
        return TELNET_CLOSE_REQUEST;
    }
    
    public static final int LENGTH = 4;
    public static final int TELNET_CLOSE = 0xfff4fffd;

    @Override
    public void encode(ByteBuf output) {
        output.writeInt(intValue());
    }
    
    public int intValue() {
        return TELNET_CLOSE;
    }
}
