package edu.uw.zookeeper.data;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Converter;
import com.google.common.primitives.UnsignedInteger;

/**
 * Converts a sequential znode label to a prefix and sequence number.
 */
public abstract class Sequential<T extends CharSequence & Comparable<? super T>, U extends Number> implements Comparable<Sequential<T,?>> {

    protected static final SequentialConverter CONVERTER = new SequentialConverter();

    @Serializes(from=String.class, to=Sequential.class)
    public static Sequential<?, ?> fromString(String input) {
        return CONVERTER.reverse().convert(input);
    }
    
    public static <T extends CharSequence & Comparable<? super T>> Sequential<T,?> fromInt(T prefix, int value) {
        if (value > 0) {
            return sequenced(prefix, UnsignedInteger.fromIntBits(value));
        } else {
            return overflowed(prefix);
        }
    }

    public static <T extends CharSequence & Comparable<? super T>> Sequenced<T> sequenced(T prefix, UnsignedInteger sequence) {
        return Sequenced.of(prefix, sequence);
    }

    public static <T extends CharSequence & Comparable<? super T>> Overflowed<T> overflowed(T prefix) {
        return Overflowed.of(prefix);
    }

    private final T prefix;
    
    protected Sequential(T prefix) {
        this.prefix = prefix;
    }

    public T prefix() {
        return prefix;
    }
    
    public abstract U sequence();

    @Serializes(from=Sequential.class, to=String.class)
    @Override
    public String toString() {
        return String.valueOf(CONVERTER.convert(this));
    }
    
    public static class SequentialConverter extends Converter<Sequential<?,?>, CharSequence> {

        public static final String SEQUENTIAL_FORMAT = "%s%010d";
        public static final Pattern OVERFLOW_PATTERN = Pattern.compile(Overflowed.OVERFLOW_SEQUENCE.toString());
        public static final Pattern SEQUENCE_PATTERN = Pattern.compile("[0-9]{10}");
        public static final Pattern SUFFIX_PATTERN = Pattern.compile("(" + OVERFLOW_PATTERN + ")|(" + SEQUENCE_PATTERN + ")");
        public static final Pattern LABEL_PATTERN = Pattern.compile("^(.+?)" + SUFFIX_PATTERN.pattern() + "$");
        
        @Override
        protected Sequential<String, ?> doBackward(CharSequence input) {
            Matcher m = LABEL_PATTERN.matcher(input);
            if (! m.matches()) {
                throw new IllegalArgumentException(String.valueOf(input));
            }
            if (m.group(2) != null) {
                return Overflowed.of(m.group(1));
            } else {
                assert (m.group(3) != null);
                return Sequenced.of(m.group(1), UnsignedInteger.valueOf(m.group(3)));
            }
        }

        @Override
        protected String doForward(Sequential<?, ?> input) {
            return String.format(Locale.ENGLISH, SEQUENTIAL_FORMAT, input.prefix(), input.sequence().intValue());
        }
    }

    public static class Sequenced<T extends CharSequence & Comparable<? super T>> extends Sequential<T, UnsignedInteger> {

        public static <T extends CharSequence & Comparable<? super T>> Sequenced<T> of(T prefix, UnsignedInteger sequence) {
            return new Sequenced<T>(prefix, sequence);
        }
        
        private final UnsignedInteger sequence;
        
        public Sequenced(T prefix, UnsignedInteger sequence) {
            super(prefix);
            this.sequence = sequence;
        }

        @Override
        public UnsignedInteger sequence() {
            return sequence;
        }

        @Override
        public int compareTo(Sequential<T, ?> other) {
            int result = prefix().compareTo(other.prefix());
            if (result == 0) {
                if (other instanceof Sequenced) {
                    return sequence.compareTo(((Sequenced<?>) other).sequence);
                } else {
                    assert (other instanceof Overflowed);
                    return -1;
                }
            }
            return result;
        }
    }

    /** 
     * From the documentation it's not clear what happens when there are
     * multiple overflows. For now we'll assume that the first one gets the
     * overflow value and the creation of the rest fails. 
     */
    public static class Overflowed<T extends CharSequence & Comparable<? super T>> extends Sequential<T, Integer> {

        public static final Integer OVERFLOW_SEQUENCE = Integer.valueOf(Integer.MIN_VALUE + 1);

        public static <T extends CharSequence & Comparable<? super T>> Overflowed<T> of(T prefix) {
            return new Overflowed<T>(prefix);
        }
        
        public Overflowed(T prefix) {
            super(prefix);
        }

        @Override
        public Integer sequence() {
            return OVERFLOW_SEQUENCE;
        }

        @Override
        public int compareTo(Sequential<T, ?> other) {
            int result = prefix().compareTo(other.prefix());
            if (result == 0) {
                if (other instanceof Overflowed) {
                    return 0;
                } else {
                    assert (other instanceof Sequenced);
                    return 1;
                }
            }
            return result;
        }
    }
}
