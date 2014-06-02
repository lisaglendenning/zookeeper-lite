package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

/**
 * Not thread-safe.
 */
public final class CountingGenerator<V> extends AbstractIterator<V> {

    public static <V> CountingGenerator<V> fromConfiguration(
            Configuration configuration,
            Generator<? extends V> callable) {
        int limit = ConfigurableIterations.get(configuration);
        int logIterations = ConfigurableLogIterations.get(configuration);
        int logInterval;
        if (logIterations > 0) {
            logInterval = limit / logIterations;
        } else {
            logInterval = 0;
        }
        return create(limit, logInterval, callable, LogManager.getLogger(CountingGenerator.class));
    }
    
    public static <V> CountingGenerator<V> create(
            int limit,
            int logInterval,
            Generator<? extends V> callable,
            Logger logger) {
        checkArgument((limit >= 0) || (limit == INFINITE_ITERATIONS), limit);
        return new CountingGenerator<V>(
                limit, logInterval, callable, logger);
    }

    public static abstract class ConfigurableInt implements Function<Configuration, Integer> {

        @Override
        public Integer apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getInt(configurable.key());
        }
    }
    
    @Configurable(arg="iterations", key="iterations", value="10", type=ConfigValueType.NUMBER)
    public static class ConfigurableIterations extends ConfigurableInt {

        public static Integer get(Configuration configuration) {
            return new ConfigurableIterations().apply(configuration);
        }
    }
    
    @Configurable(arg="logIterations", key="logIterations", value="10", type=ConfigValueType.NUMBER)
    public static class ConfigurableLogIterations extends ConfigurableInt {

        public static Integer get(Configuration configuration) {
            return new ConfigurableLogIterations().apply(configuration);
        }
    }
    
    public static final int INFINITE_ITERATIONS = -1;

    private final Logger logger;
    private final int logInterval;
    private final int limit;
    private final Generator<? extends V> callable;
    private int count;
    
    protected CountingGenerator(
            int limit,
            int logInterval,
            Generator<? extends V> callable,
            Logger logger) {
        this.logger = logger;
        this.logInterval = logInterval;
        this.limit = limit;
        this.callable = callable;
        this.count = 0;
    }
    
    public int getLimit() {
        return limit;
    }
    
    public int getCount() {
        return count;
    }
    
    @Override
    protected V computeNext() {
        if ((limit != INFINITE_ITERATIONS) && (count >= limit)) {
            return endOfData();
        }
        this.count++;
        if ((logInterval != 0) && ((count == 1) || (count == limit) || (count % logInterval == 0))) {
            logger.info("Iteration {}", count);
        }
        return callable.next();
    }
}