package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class ByTypeFactory<T> implements ParameterizedFactory<Class<? extends T>, T> {

    public static <T> ByTypeFactory<T> newInstance(Map<Class<? extends T>, Factory<? extends T>> factories) {
        return new ByTypeFactory<T>(factories);
    }
    
    private final Map<Class<? extends T>, Factory<? extends T>> factories;
    
    protected ByTypeFactory(Map<Class<? extends T>, Factory<? extends T>> factories) {
        checkArgument(! factories.isEmpty());
        this.factories = ImmutableMap.copyOf(factories);
    }
    
    @Override
    public T get(Class<? extends T> type) {
        Factory<? extends T> factory = factories.get(type);
        if (factory != null) {
            return factory.get();
        } else {
            return null;
        }
    }
}
