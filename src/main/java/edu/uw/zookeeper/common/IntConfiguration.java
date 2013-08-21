package edu.uw.zookeeper.common;

import java.util.AbstractMap;
import java.util.Map;

import com.typesafe.config.Config;

public class IntConfiguration implements DefaultsFactory<Configuration, Integer> {

    public static IntConfiguration newInstance(
            String configPath, 
            String configKey, 
            String arg,
            int defaultValue) {
        return new IntConfiguration(
                configPath, configKey, arg, defaultValue);
    }

    protected final String configPath;
    protected final String configKey;
    protected final String arg;
    protected final int defaultValue;
    
    public IntConfiguration(
            String configPath, String configKey, String arg, int defaultValue) {
        this.configPath = configPath;
        this.configKey = configKey;
        this.arg = arg;
        this.defaultValue = defaultValue;
    }
    
    @Override
    public Integer get() {
        return defaultValue;
    }

    @Override
    public Integer get(Configuration value) {
        Arguments arguments = value.asArguments();
        if (! arguments.has(arg)) {
            arguments.add(arguments.newOption(arg, "Integer"));
        }
        arguments.parse();
        Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(arg, configKey);
        @SuppressWarnings("unchecked")
        Config config = value.withArguments(configPath, args);
        if (config.hasPath(configKey)) {
            return config.getInt(configKey);
        } else {
            return get();
        }
    }   
}