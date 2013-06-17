package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

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
        } else {
            return Optional.absent();
        }
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
    public void encode(ByteBuf output) {
        output.writeInt(intValue());
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