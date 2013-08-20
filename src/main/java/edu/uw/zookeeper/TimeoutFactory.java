package edu.uw.zookeeper;

import com.typesafe.config.Config;

import edu.uw.zookeeper.common.ConfigurableTime;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.TimeValue;

public class TimeoutFactory implements DefaultsFactory<Configuration, TimeValue> {

    public static TimeoutFactory newInstance() {
        return newInstance(DEFAULT_CONFIG_PATH);
    }

    public static TimeoutFactory newInstance(String configPath) {
        return newInstance(configPath, DEFAULT_CONFIG_KEY, DEFAULT_TIMEOUT_VALUE, DEFAULT_TIMEOUT_UNIT);
    }

    public static TimeoutFactory newInstance(
            String configPath,
            String configKey,
            long defaultTimeOutValue,
            String defaultTimeOutUnit) {
        return new TimeoutFactory(configPath, configKey, defaultTimeOutValue, defaultTimeOutUnit);
    }

    public static final String DEFAULT_CONFIG_PATH = "";
    public static final String DEFAULT_CONFIG_KEY = "Timeout";
    public static final long DEFAULT_TIMEOUT_VALUE = 30;
    public static final String DEFAULT_TIMEOUT_UNIT = "SECONDS";

    protected final String configPath;
    protected final String configKey;
    protected final ConfigurableTime timeOut;

    protected TimeoutFactory(
            String configPath,
            String configKey,
            long defaultTimeOutValue,
            String defaultTimeOutUnit) {
        this.configPath = configPath;
        this.configKey = configKey;
        this.timeOut = ConfigurableTime.create(
                defaultTimeOutValue,
                defaultTimeOutUnit);
    }

    @Override
    public TimeValue get() {
        return timeOut.get();
    }

    @Override
    public TimeValue get(Configuration value) {
        Config config = value.asConfig();
        if (configPath.length() > 0 && config.hasPath(configPath)) {
            config = config.getConfig(configPath);
        }
        if (config.hasPath(configKey)) {
            return timeOut.get(config.getConfig(configKey));
        } else {
            return get();
        }
    }
}