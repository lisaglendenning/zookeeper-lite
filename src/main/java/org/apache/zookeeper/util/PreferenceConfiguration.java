package org.apache.zookeeper.util;

import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import com.google.common.base.Optional;
import com.google.common.reflect.TypeToken;
import static com.google.common.base.Preconditions.*;

public class PreferenceConfiguration implements Configuration {

    public static class PreferenceConfigurationArguments {
        public static final String OPTION_PREFS_ROOT = "prefs-root";
        public static final String OPTION_PREFS_PATH = "prefs-path";
        
        public static String OPTION_DEFAULT_PREFS_ROOT = "user";
        public static String OPTION_DEFAULT_PREFS_PATH = "org.apache.zookeeper";

        public static Arguments addArguments(Arguments arguments) {
            arguments.add(arguments.newOption(OPTION_PREFS_ROOT, 
                    Optional.of("[user,system]"), 
                    Optional.of(PreferenceConfigurationArguments.OPTION_DEFAULT_PREFS_ROOT)));
            arguments.add(arguments.newOption(OPTION_PREFS_PATH, 
                    Optional.of("PATH"), 
                    Optional.of(PreferenceConfigurationArguments.OPTION_DEFAULT_PREFS_PATH)));
            return arguments;
        }

        public static Preferences getPreferences(Arguments arguments) {
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

    protected Preferences prefs;

    public PreferenceConfiguration() {
        initialize(getClass());
    }

    public PreferenceConfiguration(Preferences prefs) {
        this.prefs = checkNotNull(prefs, "prefs");
    }

    protected PreferenceConfiguration initialize(Class<?> cls) {
        checkNotNull(cls, "cls");
        this.prefs = Preferences.userNodeForPackage(cls);
        return this;
    }

    @Override
    public PreferenceConfiguration initialize(Arguments arguments) {
        checkNotNull(arguments, "arguments");
        PreferenceConfigurationArguments.addArguments(arguments).parse();
        this.prefs = PreferenceConfigurationArguments.getPreferences(arguments);
        return this;
    }

    public Preferences getPreferences() {
        return prefs;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String name, T defaultValue) {
        checkNotNull(name, "name");
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

    @Override
    @SuppressWarnings("unchecked")
    public <T> void set(String name, T value) throws BackingStoreException {
        checkNotNull(name, "name");
        checkNotNull(value, "value");
        Class<T> cls = (Class<T>) value.getClass();
        TypeToken<T> typeToken = TypeToken.of(cls);
        if (TypeToken.of(String.class).isAssignableFrom(typeToken)) {
            prefs.put(name, (String) value);
        } else if (TypeToken.of(Boolean.class).isAssignableFrom(typeToken)) {
            prefs.putBoolean(name, (Boolean) value);
        }/*
          * else if (TypeToken.of(Byte.class).isAssignableFrom(typeToken)) {
          * prefs.putByteArray(name, (byte[]) value); }
          */else if (TypeToken.of(Double.class).isAssignableFrom(typeToken)) {
            prefs.putDouble(name, (Double) value);
        } else if (TypeToken.of(Float.class).isAssignableFrom(typeToken)) {
            prefs.putFloat(name, (Float) value);
        } else if (TypeToken.of(Integer.class).isAssignableFrom(typeToken)) {
            prefs.putInt(name, (Integer) value);
        } else if (TypeToken.of(Long.class).isAssignableFrom(typeToken)) {
            prefs.putLong(name, (Long) value);
        } else {
            throw new IllegalArgumentException();
        }
        prefs.flush();
    }
}
