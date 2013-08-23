package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public final class TimeValue implements Comparable<TimeValue> {

    public static TimeValue milliseconds(int value) {
        return milliseconds((long) value);
    }
    
    public static TimeValue milliseconds(long value) {
        return create(value, TimeUnit.MILLISECONDS);
    }
    
    public static TimeValue create(long value, String unit) {
        return create(value, TimeUnit.valueOf(unit));
    }

    public static TimeValue create(int value, TimeUnit unit) {
        return new TimeValue(value, unit);
    }

    public static TimeValue create(long value, TimeUnit unit) {
        return new TimeValue(Long.valueOf(value), unit);
    }

    protected static final ImmutableMap<TimeUnit, String> SHORT_UNIT_NAMES = Maps.immutableEnumMap( 
            new ImmutableMap.Builder<TimeUnit, String>()
                .put(TimeUnit.DAYS, "days")
                .put(TimeUnit.HOURS, "hours")
                .put(TimeUnit.MINUTES, "min")
                .put(TimeUnit.SECONDS, "s")
                .put(TimeUnit.MILLISECONDS, "ms")
                .put(TimeUnit.MICROSECONDS, "us")
                .put(TimeUnit.NANOSECONDS, "ns")
                .build());
    
    private final long value;
    private final TimeUnit unit;
    
    public TimeValue(long value, TimeUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    public long value() {
        return value;
    }

    public TimeUnit unit() {
        return unit;
    }

    public long value(TimeUnit unit) {
        return unit.convert(value, this.unit);
    }

    public TimeValue convert(TimeUnit unit) {
        return new TimeValue(value(unit), unit);
    }
    
    public TimeValue difference(TimeValue other) {
        Long diff = value - other.value(unit);
        return new TimeValue(diff, unit);
    }
    
    @Override
    public int compareTo(TimeValue other) {
        long diff = value - checkNotNull(other).value(unit);
        return (int) diff;
    }

    @Override
    public String toString() {
        return String.format("%d %s", value, SHORT_UNIT_NAMES.get(unit));
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
        return Objects.hashCode(value, unit);
    }
}
