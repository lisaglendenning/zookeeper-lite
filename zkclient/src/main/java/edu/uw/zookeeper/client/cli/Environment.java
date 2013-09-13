package edu.uw.zookeeper.client.cli;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.MapMaker;

import edu.uw.zookeeper.common.AbstractPair;

public class Environment {

    public static Environment empty() {
        return of(ImmutableMap.<Key<?>, Object>of());
    }
    
    public static Environment of(Map<Key<?>, Object> values) {
        ConcurrentMap<Key<?>, Object> env = new MapMaker().makeMap();
        env.putAll(values);
        return new Environment(env);
    }
    
    public static final class Key<T> extends AbstractPair<String, Class<?>> implements Comparable<Key<?>> {
         
        public static <T> Key<T> create(String name, Class<?> type) {
            return new Key<T>(name, type);
        }
        
        public Key(String name, Class<?> type) {
            super(name, type);
        }
        
        public String getName() {
            return first;
        }
        
        public Class<?> getType() {
            return second;
        }

        @Override
        public int compareTo(Key<?> other) {
            if (this == other) {
                return 0;
            }
            int value = first.compareTo(other.first);
            if (value == 0) {
                if (! second.equals(other.second)) {
                    value = second.hashCode() > other.second.hashCode() ? 1 : -1;
                }
            }
            return value;
        }
        
        @Override
        public String toString() {
            return first;
        }
    }

    protected final ConcurrentMap<Key<?>, Object> env;
    
    protected Environment(
            ConcurrentMap<Key<?>, Object> env) {
        this.env = env;
    }
    
    public boolean contains(Key<?> k) {
        return env.containsKey(k);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Key<T> k) {
        return (T) env.get(k);
    }

    @SuppressWarnings("unchecked")
    public <T> T put(Key<T> k, T v) {
        return (T) env.put(k, v);
    }

    @SuppressWarnings("unchecked")
    public <T> T putIfAbsent(Key<T> k, T v) {
        return (T) env.putIfAbsent(k, v);
    }
    
    @SuppressWarnings("unchecked")
    public <T> T remove(Key<T> k) {
        return (T) env.remove(k);
    }
    
    public Set<Map.Entry<Key<?>, Object>> entrySet() {
        return ImmutableSortedMap.copyOf(env).entrySet();
    }
}
