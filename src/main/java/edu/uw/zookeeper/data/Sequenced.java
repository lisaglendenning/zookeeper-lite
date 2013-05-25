package edu.uw.zookeeper.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uw.zookeeper.util.AbstractPair;

public class Sequenced extends AbstractPair<String, Integer> implements Comparable<Sequenced> {

    public static Integer sequenceOf(CharSequence input) {
        return Integer.valueOf(LABEL_PATTERN.matcher(input).group(2));
    }

    public static Sequenced of(CharSequence input) {
        Matcher m = LABEL_PATTERN.matcher(input);
        return of(m.group(1), Integer.valueOf(m.group(2)));
    }
    
    public static Sequenced of(String prefix, Integer sequence) {
        return new Sequenced(prefix, sequence);
    }
    
    public static String OVERFLOW_SEQUENCE = "-2147483647";

    public static String SEQUENCE_FORMAT = "%010d";
    public static Pattern SEQUENCE_PATTERN = Pattern.compile(OVERFLOW_SEQUENCE + "|[0-9]{10}");
    public static Pattern LABEL_PATTERN = Pattern.compile("^(?<prefix>.+?)(?<sequence>" + SEQUENCE_PATTERN.pattern() + ")$");
    
    protected Sequenced(String first, Integer second) {
        super(first, second);
    }

    public String prefix() {
        return first;
    }
    
    public Integer sequence() {
        return second;
    }
    
    @Override
    public String toString() {
        return prefix() + String.format(SEQUENCE_FORMAT, sequence());
    }

    @Override
    public int compareTo(Sequenced other) {
        int result = prefix().compareTo(other.prefix());
        if (result == 0) {
            result = sequence().compareTo(other.sequence());
        }
        return result;
    }
}
