package org.apache.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

public class Parameters implements Configurable {
    static public class Parameter<T> implements Configurable {
        protected final String key;
        protected final T defaultValue;
        protected Optional<T> value = Optional.absent();
        
        public Parameter(String key, T defaultValue) {
            this.key = checkNotNull(key);
            this.defaultValue = defaultValue;
        }

        public String getKey() {
            return key;
        }

        public T getDefaultValue() {
            return defaultValue;
        }
        
        public T getValue() {
            if (value.isPresent()) {
                return value.get();
            }
            return defaultValue;
        }

        public void setValue(T value) {
            this.value = Optional.of(value);
        }

        @Override
        public void configure(Configuration configuration) {
            setValue(configuration.get(key, defaultValue));
        }
    }
    
    public static Parameters newInstance() {
        return new Parameters();
    }

    public static <T> Parameter<T> newParameter(String key, T defaultValue) {
        return new Parameter<T>(key, defaultValue);
    }

    @SuppressWarnings("rawtypes")
    protected Map<String, Parameter> parameters = Maps.newHashMap();
    
    protected Parameters() {}

    @SuppressWarnings("rawtypes")
    protected Map<String, Parameter> delegate() {
        return parameters;
    }

    @SuppressWarnings("rawtypes")
    public Parameters add(Parameter parameter) {
        checkNotNull(parameter);
        String key = parameter.getKey();
        Map<String, Parameter> parameters = delegate();
        checkArgument(! parameters.containsKey(key));
        parameters.put(key, parameter);
        return this;
    }
    
    public <T> T getValue(String key) {
        @SuppressWarnings("unchecked")
        Parameter<T> parameter = parameters.get(key);
        checkArgument(parameter != null, "key");
        return parameter.getValue();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void configure(Configuration configuration) {
        Collection<Parameter> parameters = Collections.unmodifiableCollection(delegate().values());
        for (Parameter parameter: parameters) {
            parameter.configure(configuration);
        }
    }
}
