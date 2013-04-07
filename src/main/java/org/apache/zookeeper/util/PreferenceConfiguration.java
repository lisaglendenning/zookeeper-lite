package org.apache.zookeeper.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import com.google.common.base.Optional;
import com.google.common.reflect.TypeToken;
import static com.google.common.base.Preconditions.*;

public class PreferenceConfiguration implements Configuration {

    public static PreferenceConfiguration create() {
        return new PreferenceConfiguration();
    }

    public static class PreferenceConfigurationArguments {
        public static final String OPTION_PREFS_ROOT = "prefs-root";
        public static final String OPTION_PREFS_PATH = "prefs-path";

        public static String OPTION_DEFAULT_PREFS_ROOT = "user";
        public static String OPTION_DEFAULT_PREFS_PATH = "org.apache.zookeeper";

        public static Arguments addArguments(Arguments arguments) {
            arguments
                    .add(arguments.newOption(
                            OPTION_PREFS_ROOT,
                            Optional.of("[user,system]"),
                            Optional.of(PreferenceConfigurationArguments.OPTION_DEFAULT_PREFS_ROOT)));
            arguments
                    .add(arguments.newOption(
                            OPTION_PREFS_PATH,
                            Optional.of("PATH"),
                            Optional.of(PreferenceConfigurationArguments.OPTION_DEFAULT_PREFS_PATH)));
            return arguments;
        }

        public static Preferences getPreferences(Arguments arguments) {
            checkNotNull(arguments);
            Preferences prefs = null;
            String key = OPTION_PREFS_ROOT;
            String value = arguments.getValue(key);
            if (value == "user") {
                prefs = Preferences.userRoot();
            } else if (value == "system") {
                prefs = Preferences.systemRoot();
            } else {
                arguments.illegalValue(key, value);
            }
            key = OPTION_PREFS_PATH;
            value = arguments.getValue(key);
            prefs = prefs.node(value);
            return prefs;
        }
    }

    protected final SettableConfiguration cache;
    protected Preferences prefs;

    public PreferenceConfiguration() {
        this.cache = SettableConfiguration.create();
        initialize(getClass());
    }

    public PreferenceConfiguration(Preferences prefs) {
        this.prefs = checkNotNull(prefs);
        this.cache = SettableConfiguration.create();
    }

    protected PreferenceConfiguration initialize(Class<?> cls) {
        this.prefs = Preferences.userNodeForPackage(checkNotNull(cls));
        return this;
    }

    @Override
    public PreferenceConfiguration initialize(Arguments arguments) {
        PreferenceConfigurationArguments.addArguments(arguments).parse();
        this.prefs = PreferenceConfigurationArguments.getPreferences(arguments);
        return this;
    }

    @Override
    public Iterator<Entry<String, Object>> iterator() {
        // TODO: possible to iterate over prefs?
        return cache.iterator();
    }

    public Preferences preferences() {
        return prefs;
    }

    @Override
    public <T> T get(String name, T defaultValue) {
        T value = cache.get(name, null);
        if (value == null) {
            value = getPrefs(name, defaultValue);
            if (value != null) {
                cache.set(name, value);
            }
        }
        return value;
    }

    @Override
    public void set(String name, Object value) {
        cache.set(name, value);
    }

    @Override
    public void flush() throws BackingStoreException {
        for (Map.Entry<String, Object> opt : cache) {
            setPrefs(opt.getKey(), opt.getValue());
        }
        prefs.flush();
    }

    @SuppressWarnings("unchecked")
    protected <T> T getPrefs(String name, T defaultValue) {
        checkNotNull(name);
        T value = checkNotNull(defaultValue);
        Class<T> cls = (Class<T>) value.getClass();
        TypeToken<T> typeToken = TypeToken.of(cls);
        if (TypeToken.of(String.class).isAssignableFrom(typeToken)) {
            value = (T) prefs.get(name, (String) defaultValue);
        } else if (TypeToken.of(Boolean.class).isAssignableFrom(typeToken)) {
            value = (T) (Boolean) prefs
                    .getBoolean(name, (Boolean) defaultValue);
        }/*
          * else if (TypeToken.of(Byte.class).isAssignableFrom(typeToken)) {
          * value = (T) (byte[]) prefs.getByteArray(name, (byte[])
          * defaultValue); }
          */else if (TypeToken.of(Double.class).isAssignableFrom(typeToken)) {
            value = (T) (Double) prefs.getDouble(name, (Double) defaultValue);
        } else if (TypeToken.of(Float.class).isAssignableFrom(typeToken)) {
            value = (T) (Float) prefs.getFloat(name, (Float) defaultValue);
        } else if (TypeToken.of(Integer.class).isAssignableFrom(typeToken)) {
            value = (T) (Integer) prefs.getInt(name, (Integer) defaultValue);
        } else if (TypeToken.of(Long.class).isAssignableFrom(typeToken)) {
            value = (T) (Long) prefs.getLong(name, (Long) defaultValue);
        } else {
            throw new IllegalArgumentException();
        }
        return value;
    }

    protected void setPrefs(String name, Object value) {
        checkNotNull(name);
        checkNotNull(value);
        if (value instanceof String) {
            prefs.put(name, (String) value);
        } else if (value instanceof Boolean) {
            prefs.putBoolean(name, (Boolean) value);
        }/*
          * else if (TypeToken.of(Byte.class).isAssignableFrom(typeToken)) {
          * prefs.putByteArray(name, (byte[]) value); }
          */else if (value instanceof Double) {
            prefs.putDouble(name, (Double) value);
        } else if (value instanceof Float) {
            prefs.putFloat(name, (Float) value);
        } else if (value instanceof Integer) {
            prefs.putInt(name, (Integer) value);
        } else if (value instanceof Long) {
            prefs.putLong(name, (Long) value);
        } else {
            throw new IllegalArgumentException();
        }
    }
}
