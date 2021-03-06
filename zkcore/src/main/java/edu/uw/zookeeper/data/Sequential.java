package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Comparator;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.google.common.base.Converter;
import com.google.common.base.Optional;
import com.google.common.primitives.UnsignedInteger;

/**
 * Converts a sequential znode label to a prefix and sequence number.
 */
public abstract class Sequential<T extends CharSequence & Comparable<? super T>, U extends Number> implements Comparable<Sequential<T,?>> {

    public static final String SEQUENTIAL_FORMAT = "%s%010d";
    public static final Pattern OVERFLOW_PATTERN = Pattern.compile(Overflowed.OVERFLOW_SEQUENCE.toString());
    public static final Pattern SEQUENCE_PATTERN = Pattern.compile("[0-9]{10}");
    public static final Pattern SUFFIX_PATTERN = Pattern.compile("((" + OVERFLOW_PATTERN + ")|(" + SEQUENCE_PATTERN + "))");
    public static final Pattern LABEL_PATTERN = Pattern.compile("^(.+?)" + SUFFIX_PATTERN.pattern() + "$");

    public static Optional<? extends Sequential<String,?>> maybeFromString(CharSequence input) {
        final Matcher m = LABEL_PATTERN.matcher(input);
        if (m.matches()) {
            final String prefix = m.group(1);
            final String sequence = m.group(4);
            final Sequential<String,?> sequential;
            if (sequence != null) {
                sequential = Sequenced.valueOf(prefix, UnsignedInteger.valueOf(sequence));
            } else {
                assert (m.group(3) != null);
                sequential = Overflowed.valueOf(prefix);
            }
            return Optional.of(sequential);
        } else {
            return Optional.absent();
        }
    }

    @Serializes(from=String.class, to=Sequential.class)
    public static Sequential<String,?> fromString(CharSequence input) {
        Optional<? extends Sequential<String,?>> sequential = maybeFromString(input);
        if (sequential.isPresent()) {
            return sequential.get();
        } else {
            throw new IllegalArgumentException(String.valueOf(input));
        }
    }
    
    public static String toString(Sequential<?,?> input) {
        return String.format(Locale.ENGLISH, SEQUENTIAL_FORMAT, input.prefix(), input.sequence().intValue());
    }
    
    public static Converter<Sequential<?,?>, CharSequence> converter() {
        return new SequentialConverter();
    }
    
    public static Comparator<String> comparator() {
        return new SequentialComparator();
    }
    
    public static <T extends CharSequence & Comparable<? super T>> Sequential<T,?> fromInt(T prefix, int value) {
        if (value >= 0) {
            return sequenced(prefix, UnsignedInteger.fromIntBits(value));
        } else {
            return overflowed(prefix);
        }
    }

    public static <T extends CharSequence & Comparable<? super T>> Sequenced<T> sequenced(T prefix, UnsignedInteger sequence) {
        return Sequenced.valueOf(prefix, sequence);
    }

    public static <T extends CharSequence & Comparable<? super T>> Overflowed<T> overflowed(T prefix) {
        return Overflowed.valueOf(prefix);
    }

    private final T prefix;
    
    private Sequential(@Nonnull T prefix) {
        assert (prefix != null);
        this.prefix = prefix;
    }

    public final T prefix() {
        return prefix;
    }

    /**
     * Orders by sequence then by prefix.
     */
    @Override
    public abstract int compareTo(Sequential<T, ?> other);
    
    public abstract U sequence();

    @Serializes(from=Sequential.class, to=String.class)
    @Override
    public String toString() {
        return toString(this);
    }

    @Override
    public int hashCode() {
        return sequence().hashCode();
    }
    
    public static final class Sequenced<T extends CharSequence & Comparable<? super T>> extends Sequential<T, UnsignedInteger> {

        public static <T extends CharSequence & Comparable<? super T>> Sequenced<T> valueOf(T prefix, UnsignedInteger sequence) {
            try {
                checkNotNull(prefix).charAt(0);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException("Sequenced prefix can't be empty");
            }
            return new Sequenced<T>(prefix, checkNotNull(sequence));
        }
        
        private final UnsignedInteger sequence;
        
        private Sequenced(@Nonnull T prefix, @Nonnull UnsignedInteger sequence) {
            super(prefix);
            this.sequence = sequence;
        }

        @Override
        public UnsignedInteger sequence() {
            return sequence;
        }

        @Override
        public int compareTo(Sequential<T, ?> other) {
            int result;
            if (other instanceof Sequenced) {
                result = sequence.compareTo(((Sequenced<?>) other).sequence);
                if (result == 0) {
                    result = prefix().compareTo(other.prefix());
                }
            } else {
                assert (other instanceof Overflowed);
                result = -1;
            }
            return result;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Sequenced)) {
                return false;
            }
            Sequenced<?> other = (Sequenced<?>) obj;
            return sequence().equals(other.sequence())
                    && prefix().equals(other.prefix());
        }
    }

    /** 
     * From the documentation it's not clear what happens when there are
     * multiple overflows. For now we'll assume that the first one gets the
     * overflow value and the creation of the rest fails. 
     */
    public static final class Overflowed<T extends CharSequence & Comparable<? super T>> extends Sequential<T, Integer> {

        public static final Integer OVERFLOW_SEQUENCE = Integer.valueOf(Integer.MIN_VALUE + 1);

        public static <T extends CharSequence & Comparable<? super T>> Overflowed<T> valueOf(T prefix) {
            try {
                checkNotNull(prefix).charAt(0);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException("Overflowed prefix can't be empty");
            }
            return new Overflowed<T>(checkNotNull(prefix));
        }
        
        private Overflowed(@Nonnull T prefix) {
            super(prefix);
        }

        @Override
        public Integer sequence() {
            return OVERFLOW_SEQUENCE;
        }

        @Override
        public int compareTo(Sequential<T, ?> other) {
            int result;
            if (other instanceof Sequenced) {
                result = 1;
            } else {
                assert (other instanceof Overflowed);
                result = prefix().compareTo(other.prefix());
            }
            return result;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Overflowed)) {
                return false;
            }
            return prefix().equals(((Overflowed<?>) obj).prefix());
        }
    }

    public static final class SequentialConverter extends Converter<Sequential<?,?>, CharSequence> {
    
        public SequentialConverter() {}
        
        @Override
        protected Sequential<String, ?> doBackward(CharSequence input) {
            return fromString(input);
        }
    
        @Override
        protected String doForward(Sequential<?,?> input) {
            return input.toString();
        }
    }

    /**
     * Uses the inherent ordering of Strings and Sequentials.
     * For mixed cases, compares the prefix of the Sequential to the String.
     * if the prefix of the sequential equals the string,
     * then the sequential is ordered higher.
     */
    public static final class SequentialComparator implements Comparator<String> {

        public SequentialComparator() {}
        
        @Override
        public int compare(String o1, String o2) {
            Optional<? extends Sequential<String,?>> s1 = maybeFromString(o1);
            Optional<? extends Sequential<String,?>> s2 = maybeFromString(o2);
            if (s1.isPresent()) {
                if (s2.isPresent()) {
                    return (s1.get().compareTo(s2.get()));
                } else {
                    int cmp = s1.get().prefix().compareTo(o2);
                    if (cmp == 0) {
                        cmp = 1;
                    }
                    return cmp;
                }
            } else {
                if (s2.isPresent()) {
                    int cmp = o1.compareTo(s2.get().prefix());
                    if (cmp == 0) {
                        cmp = -1;
                    }
                    return cmp;
                } else {
                    return o1.compareTo(o2);
                }
            }
        }
    }
}
