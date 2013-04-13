package org.apache.zookeeper.util;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;

public class TimeValue extends Pair<Long, TimeUnit> {

    public static TimeValue create(Long value, String unit) {
        return new TimeValue(value, unit);
    }

    public static TimeValue create(Long value, TimeUnit unit) {
        return new TimeValue(value, unit);
    }
    
    public TimeValue(Long value, String unit) {
        this(value, TimeUnit.valueOf(unit));
    }
    
    public TimeValue(Long value, TimeUnit unit) {
        super(value, unit);
    }

    public Long value() {
        return first();
    }

    public TimeUnit unit() {
        return second();
    }

    public Long value(TimeUnit unit) {
        return unit.convert(value(), unit());
    }

    public TimeValue convert(TimeUnit unit) {
        return new TimeValue(value(unit), unit);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("value", value())
                .add("unit", unit().name())
                .toString();
    }
}
