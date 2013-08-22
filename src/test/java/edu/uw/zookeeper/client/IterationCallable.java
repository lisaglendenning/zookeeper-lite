package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.IntConfiguration;

public class IterationCallable<V> implements Callable<Optional<V>> {

    public static <V> IterationCallable<V> create(
            Configuration configuration,
            Callable<V> callable) {
        return create(newConfiguration().get(configuration), callable);
    }
    
    public static <V> IterationCallable<V> create(
            int iterations,
            Callable<V> callable) {
        return new IterationCallable<V>(iterations, callable);
    }

    public static IntConfiguration newConfiguration() {
        return newConfiguration(DEFAULT_CONFIG_PATH);
    }
    
    public static IntConfiguration newConfiguration(String configPath) {
        return IntConfiguration.newInstance(
                configPath, DEFAULT_CONFIG_KEY, DEFAULT_CONFIG_ARG, DEFAULT_CONFIG_VALUE);
    }

    public static final String DEFAULT_CONFIG_PATH = "";
    public static final String DEFAULT_CONFIG_ARG = "iterations";
    public static final String DEFAULT_CONFIG_KEY = "Iterations";
    public static final int DEFAULT_CONFIG_VALUE = 5;

    protected final int iterations;
    protected final AtomicInteger count;
    protected final Callable<V> callable;
    
    public IterationCallable(
            int iterations,
            Callable<V> callable) {
        checkArgument(iterations >= 0);
        this.iterations = iterations;
        this.count = new AtomicInteger(0);
        this.callable = callable;
    }
    
    public int getIterations() {
        return iterations;
    }
    
    public int getCount() {
        return count.get();
    }
    
    @Override
    public Optional<V> call() throws Exception {
        final int count = this.count.incrementAndGet();
        checkState(count <= iterations);
        V result = callable.call();
        if (count < iterations) {
            return Optional.absent();
        } else {
            return Optional.of(result);
        }
    }
}