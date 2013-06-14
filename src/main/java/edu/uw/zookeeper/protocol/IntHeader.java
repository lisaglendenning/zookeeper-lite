package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

public class IntHeader implements Encodable {

    public static IntHeader of(int intValue) {
        return new IntHeader(intValue);
    }

    public static Optional<IntHeader> decode(ByteBuf input) {
        if (input.readableBytes() >= IntHeader.length()) {
            int value = input.readInt();
            return Optional.of(IntHeader.of(value));
        }
        return Optional.absent();
    }

    private static final int LENGTH = 4;

    public static int length() {
        return LENGTH;
    }
    
    private final int intValue;
    
    public IntHeader(int intValue) {
        this.intValue = intValue;
    }
    
    public int intValue() {
        return intValue;
    }

    @Override
    public ByteBuf encode(ByteBufAllocator output) {
        ByteBuf out = output.buffer(length(), length());
        out.writeInt(intValue());
        return out;
    }
    
    @Override
    public String toString() {
        return String.valueOf(intValue());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof IntHeader)) {
            return false;
        }
        IntHeader other = (IntHeader) obj;
        return Objects.equal(intValue(), other.intValue());
    }
    
    @Override
    public int hashCode() {
        return intValue();
    }
}