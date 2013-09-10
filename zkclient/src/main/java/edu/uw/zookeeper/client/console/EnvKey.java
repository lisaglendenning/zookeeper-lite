package edu.uw.zookeeper.client.console;

import java.util.Map;

public enum EnvKey {
    PROMPT, CWD, MULTI;
    
    @SuppressWarnings("unchecked")
    public <T> T get(Map<String, ? super T> env) {
        return (T) env.get(name());
    }

    @SuppressWarnings("unchecked")
    public <T> T put(Map<String, ? super T> env, T value) {
        return (T) env.put(name(), value);
    }
    
    public boolean contains(Map<String, ?> env) {
        return env.containsKey(name());
    }

    @SuppressWarnings("unchecked")
    public <T> T remove(Map<String, ?> env) {
        return (T) env.remove(name());
    }
}