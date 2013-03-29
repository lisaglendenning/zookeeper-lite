package org.apache.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public enum FourLetterCommand {
    
    CONF("conf") {},
    CONS("cons") {},
    CRST("crst") {},
    DUMP("dump") {},
    ENVI("envi") {},
    GTMK("gtmk") {},
    ISRO("isro") {},
    MNTR("mntr") {},
    RUOK("ruok") {},
    STMK("stmk") {},
    SRST("srst") {},
    SRVR("srvr") {},
    STAT("stat") {},
    WCHC("wchc") {},
    WCHP("wchp") {},
    WCHS("wchs") {};

    public static final int LENGTH = 4;
    // TODO: know we want single-byte encoding, but is it US-ASCII or UTF-8?
    public static final String CHARSET = "US-ASCII";
    
    // TODO: immutable?
    protected final static Map<String, FourLetterCommand> wordToCommand = Maps.newHashMap();
    static {
        for (FourLetterCommand item : FourLetterCommand.values()) {
            String word = item.word();
            assert (!(wordToCommand.containsKey(word)));
            wordToCommand.put(word, item);
        }
    }

    protected final String word;
    protected final List<Byte> bytes; // List<> because byte[] is mutable
    
    public static byte[] encode(String word) {
        try {
            return word.getBytes(CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
    
    public static String decode(byte[] bytes) {
        try {
            return new String(bytes, CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
    
    public static boolean isWord(String word) {
        return wordToCommand.containsKey(word);
    }

    public static boolean isWord(byte[] bytes) {
        String word = decode(bytes);
        return wordToCommand.containsKey(word);
    }
    
    public static FourLetterCommand fromWord(String word) {
        checkArgument(isWord(word));
        return wordToCommand.get(word);
    }

    public static FourLetterCommand fromWord(byte[] bytes) {
        String word = decode(bytes);
        return fromWord(word);
    }
    
    private FourLetterCommand(String word) {
        assert (word.length() == LENGTH);
        this.word = word;
        byte[] bytes = encode(word);
        // don't store byte[] directly because it is mutable
        // must be a nicer way to do this conversion
        Byte[] bytesWrapper = new Byte[bytes.length];
        for (int i=0; i<bytes.length; ++i) {
            bytesWrapper[i] = bytes[i];
        }
        this.bytes = ImmutableList.copyOf(bytesWrapper);
    }
    
    public int length() {
        return word().length();
    }
    
    public String word() {
        return word;
    }
    
    public byte[] bytes() {
        byte[] bytesWrapper = new byte[bytes.size()];
        for (int i=0; i<bytes.size(); ++i) {
            bytesWrapper[i] = bytes.get(i);
        }
        return bytesWrapper;
    }
}
