package edu.uw.zookeeper;

import com.google.common.base.Function;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TimeValue;

@Configurable(arg="timeout", value="30 seconds", help="time")
public class ConfigurableTimeout implements Function<Configuration, TimeValue> {

    public static TimeValue get(Configuration configuration) {
        return new ConfigurableTimeout().apply(configuration);
    }

    @Override
    public TimeValue apply(Configuration configuration) {
        Configurable configurable = getClass().getAnnotation(Configurable.class);
        return TimeValue.fromString(
                configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path())
                        .getString(configurable.arg()));
    }
}
