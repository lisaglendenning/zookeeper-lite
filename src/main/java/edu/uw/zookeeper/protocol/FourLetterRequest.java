package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.*;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

public enum FourLetterRequest implements Message.ClientMessage {

    CONF("conf") {
    },
    CONS("cons") {
    },
    CRST("crst") {
    },
    DUMP("dump") {
    },
    ENVI("envi") {
    },
    GTMK("gtmk") {
    },
    ISRO("isro") {
    },
    MNTR("mntr") {
    },
    RUOK("ruok") {
    },
    STMK("stmk") {
    },
    SRST("srst") {
    },
    SRVR("srvr") {
    },
    STAT("stat") {
    },
    WCHC("wchc") {
    },
    WCHP("wchp") {
    },
    WCHS("wchs") {
    };

    public static Optional<FourLetterRequest> decode(ByteBuf input) {
        if (checkNotNull(input).readableBytes() >= LENGTH) {
            byte[] bytes = new byte[LENGTH];
            input.getBytes(input.readerIndex(), bytes);
            if (FourLetterRequest.has(bytes)) {
                FourLetterRequest command = FourLetterRequest.of(bytes);
                input.skipBytes(bytes.length);
                return Optional.of(command);
            }
        }
        return Optional.absent();
    }

    public static int LENGTH = 4;
    
    private final static ImmutableMap<String, FourLetterRequest> byWord = Maps
            .uniqueIndex(Iterators.forArray(FourLetterRequest.values()), 
                    new Function<FourLetterRequest, String>() {
                        @Override public String apply(FourLetterRequest input) {
                            return input.word();
                        }});

    public static Charset encoding() {
        // TODO: know we want single-byte encoding, but is it US-ASCII or UTF-8?
        return Charset.forName("US-ASCII");
    }
    
    public static byte[] encode(String word) {
        return word.getBytes(encoding());
    }

    public static String decode(byte[] bytes) {
        return new String(bytes, encoding());
    }

    public static boolean has(String word) {
        return byWord.containsKey(word);
    }

    public static boolean has(byte[] bytes) {
        String word = decode(bytes);
        return byWord.containsKey(word);
    }

    public static FourLetterRequest of(String word) {
        checkArgument(has(word));
        return byWord.get(word);
    }

    public static FourLetterRequest of(byte[] bytes) {
        String word = decode(bytes);
        return of(word);
    }

    private final String word;
    
    private FourLetterRequest(String word) {
        this.word = word;
    }

    public int length() {
        return LENGTH;
    }

    public String word() {
        return word;
    }

    public byte[] bytes() {
        return encode(word);
    }

    @Override
    public void encode(ByteBuf output) {
        output.writeBytes(bytes());
    }
}
