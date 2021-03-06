package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.buffer.ByteBuf;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

public class FourLetterResponse implements Message.ServerAnonymous {

    public static FourLetterResponse fromString(String strValue) {
        return new FourLetterResponse(strValue);
    }

    public static FourLetterResponse fromBytes(byte[] byteValue) {
        return new FourLetterResponse(byteValue);
    }

    public static Charset encoding() {
        return FourLetterWord.encoding();
    }

    public static Optional<FourLetterResponse> decode(ByteBuf input) {
        int length = checkNotNull(input).readableBytes();
        if (length > 0) {
            byte[] bytes = new byte[length];
            input.readBytes(bytes);
            FourLetterResponse response = FourLetterResponse.fromBytes(bytes);
            return Optional.of(response);
        }
        return Optional.absent();
    }

    private final String strValue;

    private FourLetterResponse(String strValue) {
        this.strValue = strValue;
    }

    private FourLetterResponse(byte[] byteValue) {
        try {
            this.strValue = new String(byteValue, encoding().name());
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError();
        }
    }
    
    public String stringValue() {
        return strValue;
    }
    
    public byte[] byteValue() {
        return strValue.getBytes(encoding());
    }

    @Override
    public void encode(ByteBuf output) {
        output.writeBytes(byteValue());
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(stringValue()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(stringValue());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FourLetterResponse other = (FourLetterResponse) obj;
        return Objects.equal(stringValue(), other.stringValue());
    }
}
