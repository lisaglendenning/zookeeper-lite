package org.apache.zookeeper.util;

import java.util.concurrent.TimeUnit;

public class ConfigurableTime implements Configurable {
    
    public static ConfigurableTime create(
            String valueKey, long defaultValue,
            String unitKey, String defaultUnit) {
        return new ConfigurableTime(valueKey, defaultValue, unitKey, defaultUnit);
    }
    
    public final Parameters.Parameter<Long> PARAM_VALUE;
    public final Parameters.Parameter<String> PARAM_UNIT;
    public final Parameters parameters;
    
    public ConfigurableTime(
            String valueKey, long defaultValue,
            String unitKey, String defaultUnit) {
        this.PARAM_VALUE = Parameters.newParameter(valueKey, defaultValue);
        this.PARAM_UNIT = Parameters.newParameter(unitKey, defaultUnit);
        this.parameters = Parameters.newInstance()
                .add(PARAM_VALUE).add(PARAM_UNIT);
    }
    
    @Override
    public void configure(Configuration configuration) {
        parameters.configure(configuration);
        TimeUnit.valueOf(PARAM_UNIT.getValue());
    }

    public long time() {
        return PARAM_VALUE.getValue();
    }
    
    public TimeUnit timeUnit() {
        return TimeUnit.valueOf(PARAM_UNIT.getValue());
    }
    
    public long convert(TimeUnit unit) {
        return unit.convert(time(), timeUnit());
    }
}
