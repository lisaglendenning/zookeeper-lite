package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

public class Hex {
    
    public static String toPaddedHexString(long value) {
        return toPaddedHexString(Long.toHexString(value), Longs.BYTES*2);
    }
    
    public static String toPaddedHexString(int value) {
        return toPaddedHexString(Integer.toHexString(value), Ints.BYTES*2);
    }

    public static String toPaddedHexString(short value) {
        return toPaddedHexString(Integer.toHexString(((int) value) & 0xffff), Shorts.BYTES*2);
    }
    
    public static String toPaddedHexString(String value, int length) {
        checkArgument(value.length() <= length);
        StringBuilder sb = new StringBuilder(length);
        for (int toPrepend=sb.capacity()-value.length(); toPrepend>0; --toPrepend) {
            sb.append('0');
        }
        sb.append(value);
        return sb.toString();
    }
    
    public static long parseLong(String string) {
        return Long.parseLong(string, 16);
    }
    
    public static int parseInt(String string) {
        return Integer.parseInt(string, 16);
    }
    
    public static short parseShort(String string) {
        return Short.parseShort(string, 16);
    }
}
