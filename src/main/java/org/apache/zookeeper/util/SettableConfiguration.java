package org.apache.zookeeper.util;

import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.inject.AbstractModule;

import static com.google.common.base.Preconditions.*;

public class SettableConfiguration implements Configuration {

    protected final Map<String, String> options;

    public SettableConfiguration() {
        options = Maps.newHashMap();
    }

    public SettableConfiguration(Arguments arguments) {
        this();
        initialize(arguments);
    }

    @Override
    public SettableConfiguration initialize(Arguments arguments) {
        checkNotNull(arguments);
        for (Arguments.Option option: arguments) {
            if (option.hasValue()) {
                options.put(option.getName(), option.getValue());
            }
        }
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String name, T defaultValue) {
        checkNotNull(name, "name");
        T value = checkNotNull(defaultValue);
        if (! options.containsKey(name)) {
            return value;
        }
        String valueStr = options.get(name);
        Class<T> cls = (Class<T>) value.getClass();
        TypeToken<T> typeToken = TypeToken.of(cls);
        if (TypeToken.of(String.class).isAssignableFrom(typeToken)) {
            value = (T) valueStr;
        } else if (TypeToken.of(Boolean.class).isAssignableFrom(typeToken)) {
            value = (T) new Boolean(valueStr);
        }/*
          * else if (TypeToken.of(Byte.class).isAssignableFrom(typeToken)) {
          * value = (T) (byte[]) prefs.getByteArray(name, (byte[])
          * defaultValue); }
          */else if (TypeToken.of(Double.class).isAssignableFrom(typeToken)) {
            value = (T) new Double(valueStr);
        } else if (TypeToken.of(Float.class).isAssignableFrom(typeToken)) {
            value = (T) new Float(valueStr);
        } else if (TypeToken.of(Integer.class).isAssignableFrom(typeToken)) {
            value = (T) new Integer(valueStr);
        } else if (TypeToken.of(Long.class).isAssignableFrom(typeToken)) {
            value = (T) new Long(valueStr);
        } else {
            throw new IllegalArgumentException();
        }
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void set(String name, T value) {
        checkNotNull(name, "name");
        checkNotNull(value, "value");
        String valueStr = value.toString();
        options.put(name, valueStr);
    }
}
