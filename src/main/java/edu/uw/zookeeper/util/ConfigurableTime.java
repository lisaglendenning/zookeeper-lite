package edu.uw.zookeeper.util;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

public class ConfigurableTime extends ConfigurableReference<TimeValue> {
    
    public static ConfigurableTime create(long value, String unit) {
        Factory factory = Factory.create(value, unit);
        return new ConfigurableTime(factory);
    }

    public static ConfigurableTime create(long value) {
        Factory factory = Factory.create(value);
        return new ConfigurableTime(factory);
    }

    protected ConfigurableTime(ConfigurableFactory<TimeValue> configurable) {
        super(configurable);
    }
    
    public static class Factory extends AbstractConfigurableFactory<TimeValue> {
        public static final String DEFAULT_UNIT = TimeUnit.MILLISECONDS.toString();
        
        public static final String KEY_VALUE = "value";
        public static final String KEY_UNIT = "unit";

        public static Factory create(long value) {
            return create(value, DEFAULT_UNIT);
        }

        public static Factory create(long value, String unit) {
            return new Factory(value, unit);
        }

        protected Factory(long value, String unit) {
            super(ImmutableMap.<String, Object>builder()
                    .put(KEY_VALUE, value)
                    .put(KEY_UNIT, unit)
                    .build());
        }
        
        @Override
        public TimeValue get(Config config) {
            if (config != defaults()) {
                config = config.withFallback(defaults());
            }
            return new TimeValue(config.getLong(KEY_VALUE),
                    config.getString(KEY_UNIT));
        }
    }
}
