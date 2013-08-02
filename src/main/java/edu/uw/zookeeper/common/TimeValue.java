package edu.uw.zookeeper.common;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class TimeValue extends AbstractPair<Long, TimeUnit> implements Comparable<TimeValue> {

    public static TimeValue create(Long value, String unit) {
        return new TimeValue(value, unit);
    }

    public static TimeValue create(Long value, TimeUnit unit) {
        return new TimeValue(value, unit);
    }

    private static final ImmutableMap<TimeUnit, String> SHORT_UNIT_NAMES = Maps.immutableEnumMap( 
            new ImmutableMap.Builder<TimeUnit, String>()
                .put(TimeUnit.DAYS, "days")
                .put(TimeUnit.HOURS, "hours")
                .put(TimeUnit.MINUTES, "min")
                .put(TimeUnit.SECONDS, "s")
                .put(TimeUnit.MILLISECONDS, "ms")
                .put(TimeUnit.MICROSECONDS, "us")
                .put(TimeUnit.NANOSECONDS, "ns")
                .build());
    
    public TimeValue(Long value, String unit) {
        this(value, TimeUnit.valueOf(unit));
    }
    
    public TimeValue(Long value, TimeUnit unit) {
        super(value, unit);
    }

    public Long value() {
        return first;
    }

    public TimeUnit unit() {
        return second;
    }

    public Long value(TimeUnit unit) {
        return unit.convert(value(), unit());
    }

    public TimeValue convert(TimeUnit unit) {
        return new TimeValue(value(unit), unit);
    }
    
    public TimeValue difference(TimeValue other) {
        Long diff = value() - other.value(unit());
        return TimeValue.create(diff, unit());
    }
    
    @Override
    public int compareTo(TimeValue other) {
        Long diff = value() - other.value(unit());
        return diff.intValue();
    }

    @Override
    public String toString() {
        return String.format("%d %s", value(), SHORT_UNIT_NAMES.get(unit()));
    }
}
