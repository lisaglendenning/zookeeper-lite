package edu.uw.zookeeper.util;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

public class ConfigurableTime extends AbstractConfigurableFactory<TimeValue> {

    public static final String DEFAULT_UNIT = TimeUnit.MILLISECONDS.toString();
    
    public static final String KEY_VALUE = "value";
    public static final String KEY_UNIT = "unit";

    public static ConfigurableTime create(long value) {
        return create(value, DEFAULT_UNIT);
    }

    public static ConfigurableTime create(long value, String unit) {
        return new ConfigurableTime(value, unit);
    }

    private ConfigurableTime(long value, String unit) {
        super(ImmutableMap.<String, Object>builder()
                .put(KEY_VALUE, value)
                .put(KEY_UNIT, unit)
                .build());
    }

    @Override
    protected TimeValue fromConfig(Config config) {
        return new TimeValue(config.getLong(KEY_VALUE),
                config.getString(KEY_UNIT));
    }
}
