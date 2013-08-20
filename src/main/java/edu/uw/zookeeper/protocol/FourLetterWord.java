package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.*;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

public enum FourLetterWord implements Encodable {

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

    public static int LENGTH = 4;
    
    private final static ImmutableMap<String, FourLetterWord> byWord = Maps
            .uniqueIndex(Iterators.forArray(FourLetterWord.values()), 
                    new Function<FourLetterWord, String>() {
                        @Override public String apply(FourLetterWord input) {
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

    public static FourLetterWord of(String word) {
        checkArgument(has(word));
        return byWord.get(word);
    }

    public static FourLetterWord of(byte[] bytes) {
        String word = decode(bytes);
        return of(word);
    }

    private final String word;
    
    private FourLetterWord(String word) {
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
