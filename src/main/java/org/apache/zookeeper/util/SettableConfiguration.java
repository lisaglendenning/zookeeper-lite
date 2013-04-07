package org.apache.zookeeper.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import static com.google.common.base.Preconditions.*;

public class SettableConfiguration implements Configuration {

    public static SettableConfiguration create() {
        return new SettableConfiguration();
    }
    
    protected final Map<String, Object> options;

    public SettableConfiguration() {
        this.options = Collections.synchronizedMap(Maps.<String, Object>newHashMap());
    }

    public SettableConfiguration(Arguments arguments) {
        this();
        initialize(arguments);
    }

    @Override
    public SettableConfiguration initialize(Arguments arguments) {
        for (Arguments.Option option: checkNotNull(arguments)) {
            if (option.hasValue()) {
                options.put(option.getName(), option.getValue());
            }
        }
        return this;
    }

    @Override
    public Iterator<Entry<String, Object>> iterator() {
        return options.entrySet().iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String name, T defaultValue) {
        Object storedValue = options.get(name);
        if (storedValue == null) {
            return defaultValue;
        }
        
        T value = null;
        try {
            value = (T) storedValue;
            return value;
        } catch (ClassCastException e) {
        }
        
        checkArgument(storedValue instanceof String);
        String valueStr = (String) storedValue;
        
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
    public void set(String name, Object value) {
        options.put(checkNotNull(name), checkNotNull(value));
    }
    
    @Override
    public void flush() {
    }
}
