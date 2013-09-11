package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.*;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

public enum FourLetterWord implements Encodable {
    CONF,
    CONS,
    CRST,
    DUMP,
    ENVI,
    GTMK,
    ISRO,
    MNTR,
    RUOK,
    STMK,
    SRST,
    SRVR,
    STAT,
    WCHC,
    WCHP,
    WCHS;

    private static final int LENGTH = 4;
    
    // TODO: know we want single-byte encoding, but is it US-ASCII or UTF-8?
    private static final Charset ENCODING = Charsets.US_ASCII;
    
    private final static ImmutableBiMap<String, FourLetterWord> words = ImmutableBiMap.copyOf(
            Maps.uniqueIndex(
                    Iterators.forArray(FourLetterWord.values()), 
                    new Function<FourLetterWord, String>() {
                        @Override public String apply(FourLetterWord input) {
                            return input.name().toLowerCase();
                        }}));

    private final static ImmutableMap<FourLetterWord, byte[]> bytes = Maps.toMap(
            Iterators.forArray(FourLetterWord.values()), 
            new Function<FourLetterWord, byte[]>() {
                @Override public byte[] apply(FourLetterWord input) {
                    return encodeString(input.toString());
                }});
    
    public static int length() {
        return LENGTH;
    }
    
    public static Charset encoding() {
        return ENCODING;
    }
    
    public static byte[] encodeString(String word) {
        return word.getBytes(encoding());
    }

    public static String decodeString(byte[] bytes) {
        return new String(bytes, encoding());
    }

    public static boolean has(String word) {
        return words.containsKey(word);
    }

    public static boolean has(byte[] bytes) {
        return has(decodeString(bytes));
    }

    public static FourLetterWord of(String word) {
        return words.get(word);
    }

    public static FourLetterWord of(byte[] bytes) {
        return of(decodeString(bytes));
    }

    public static Optional<FourLetterWord> decode(ByteBuf input) {
        if (checkNotNull(input).readableBytes() >= LENGTH) {
            byte[] bytes = new byte[LENGTH];
            input.getBytes(input.readerIndex(), bytes);
            String word = decodeString(bytes);
            if (FourLetterWord.has(word)) {
                FourLetterWord command = FourLetterWord.of(word);
                input.skipBytes(LENGTH);
                return Optional.of(command);
            }
        }
        return Optional.absent();
    }

    private FourLetterWord() {
    }

    public byte[] bytes() {
        return bytes.get(this);
    }

    @Override
    public void encode(ByteBuf output) {
        output.writeBytes(bytes());
    }
    
    @Override
    public String toString() {
        return words.inverse().get(this);
    }
}
