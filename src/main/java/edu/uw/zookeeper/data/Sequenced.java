package edu.uw.zookeeper.data;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uw.zookeeper.util.AbstractPair;

public class Sequenced<T extends CharSequence & Comparable<? super T>> extends AbstractPair<T, Integer> implements Comparable<Sequenced<T>> {

    public static Integer sequenceOf(CharSequence input) {
        return Integer.valueOf(LABEL_PATTERN.matcher(input).group(2));
    }
    
    public static String toString(CharSequence prefix, int sequence) {
        return prefix + String.format(Locale.ENGLISH, SEQUENCE_FORMAT, sequence);
    }

    public static Sequenced<String> of(CharSequence input) {
        Matcher m = LABEL_PATTERN.matcher(input);
        if (! m.matches()) {
            throw new IllegalArgumentException(String.valueOf(input));
        }
        return of(m.group(1), Integer.valueOf(m.group(2)));
    }
    
    public static <T extends CharSequence & Comparable<? super T>> Sequenced<T> of(T prefix, Integer sequence) {
        return new Sequenced<T>(prefix, sequence);
    }
    
    public static final String OVERFLOW_SEQUENCE = "-2147483647";
    public static final String SEQUENCE_FORMAT = "%010d";
    public static final Pattern SEQUENCE_PATTERN = Pattern.compile(OVERFLOW_SEQUENCE + "|[0-9]{10}");
    public static final Pattern LABEL_PATTERN = Pattern.compile("^(.+?)(" + SEQUENCE_PATTERN.pattern() + ")$");
    
    protected Sequenced(T first, Integer second) {
        super(first, second);
    }

    public T prefix() {
        return first;
    }
    
    public Integer sequence() {
        return second;
    }
    
    @Override
    public String toString() {
        return toString(prefix(), sequence());
    }

    @Override
    public int compareTo(Sequenced<T> other) {
        int result = prefix().compareTo(other.prefix());
        if (result == 0) {
            result = sequence().compareTo(other.sequence());
        }
        return result;
    }
}
