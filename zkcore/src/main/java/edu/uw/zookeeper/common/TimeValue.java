package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableBiMap;

import edu.uw.zookeeper.data.Serializes;

public final class TimeValue implements Comparable<TimeValue> {

    public static TimeValue milliseconds(long value) {
        return new TimeValue(value, TimeUnit.MILLISECONDS);
    }

    public static TimeValue seconds(long value) {
        return new TimeValue(value, TimeUnit.SECONDS);
    }
    
    public static TimeValue create(long value, String unitText) {
        checkNotNull(unitText);
        TimeUnit unit = null;
        try {
            unit = TimeUnit.valueOf(unitText.toUpperCase());
        } catch (IllegalArgumentException e) {
            unit = SHORT_UNIT_NAMES.inverse().get(unitText.toLowerCase());
        }
        if (unit == null) {
            throw new IllegalArgumentException(String.valueOf(unitText));
        }
        return new TimeValue(value, unit);
    }

    public static TimeValue create(long value, TimeUnit unit) {
        return new TimeValue(value, unit);
    }
    
    @Serializes(from=String.class, to=TimeValue.class)
    public static TimeValue fromString(String text) {
        Iterator<String> fields = SPLITTER.split(text).iterator();
        long value = Long.parseLong(fields.next());
        String unit = fields.next();
        return create(value, unit);
    }

    public static final ImmutableBiMap<TimeUnit, String> SHORT_UNIT_NAMES = 
            ImmutableBiMap.<TimeUnit, String>builder() 
                .put(TimeUnit.DAYS, "d")
                .put(TimeUnit.HOURS, "h")
                .put(TimeUnit.MINUTES, "m")
                .put(TimeUnit.SECONDS, "s")
                .put(TimeUnit.MILLISECONDS, "ms")
                .put(TimeUnit.MICROSECONDS, "us")
                .put(TimeUnit.NANOSECONDS, "ns")
                .build();
    
    protected static String FORMAT = "%d %s";
    protected static Splitter SPLITTER = Splitter.on(Pattern.compile("[ \t]+")).omitEmptyStrings().limit(2);
    
    private final long value;
    private final TimeUnit unit;
    
    public TimeValue(long value, TimeUnit unit) {
        this.value = value;
        this.unit = checkNotNull(unit);
    }

    public long value() {
        return value;
    }

    public TimeUnit unit() {
        return unit;
    }

    public long value(TimeUnit to) {
        return to.convert(value, unit);
    }

    public TimeValue convert(TimeUnit to) {
        return new TimeValue(value(to), to);
    }
    
    public TimeValue difference(TimeValue other) {
        Long diff = value - other.value(unit);
        return new TimeValue(diff, unit);
    }
    
    @Override
    public int compareTo(TimeValue other) {
        long diff = value - other.value(unit);
        return (int) diff;
    }

    @Override
    public String toString() {
        return String.format(FORMAT, value, SHORT_UNIT_NAMES.get(unit));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof TimeValue)) {
            return false;
        }
        TimeValue other = (TimeValue) obj;
        return Objects.equal(value, other.value)
                && Objects.equal(unit, other.unit);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}
