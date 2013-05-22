package edu.uw.zookeeper.data;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public enum Serializers {
    REGISTRY;
    
    public static Serializers getInstance() {
        return REGISTRY;
    }

    @SuppressWarnings("unchecked")
    public <I,O> O toClass(I input, Class<O> outputType) {
        Class<?> inputType = input.getClass();
        O output;
        Serializers.Serializer method = find(outputType, inputType, outputType);
        try {
            output = (O) method.method().invoke(outputType, input);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return output;
    }
    
    public static enum ToString implements Function<Object, String> {
        TO_STRING {
            @Override
            public String apply(Object input) {
                return Serializers.getInstance().toClass(input, String.class);
            }
        };
    }
    
    protected final ConcurrentMap<Class<?>, List<Serializer>> registry;
    
    private Serializers() {
        this.registry = Maps.newConcurrentMap();
    }
    
    public List<Serializer> add(Class<?> type) {
        List<Serializer> serializers = registry.get(type);
        if (serializers == null) {
            serializers = Serializer.discover(type);
            registry.put(type, serializers);
        }
        return serializers;
    }
    
    public List<Serializer> get(Class<?> type) {
        return registry.get(type);
    }

    public Serializer find(Class<?> type,
            Class<?> inputType, Class<?> outputType) {
        List<Serializer> serializers = add(type);
        return Serializer.find(serializers, inputType, outputType);
    }
    
    public static class Serializer {

        public static List<Serializer> discover(Class<?> type) {
            List<Serializer> serializers = Lists.newLinkedList();
            for (Method method: type.getMethods()) {
                Serializes annotation = method.getAnnotation(Serializes.class);
                if (annotation == null) {
                    continue;
                }
                Class<?> input = annotation.from();
                if (input.equals(Void.class)) {
                    Class<?>[] parameterTypes = method.getParameterTypes();
                    if (parameterTypes.length == 0) {
                        input = type;
                    } else if (parameterTypes.length == 1) {
                        input = parameterTypes[0];
                    } else {
                        // ?
                    }
                }
                Class<?> output = annotation.to();
                if (output.equals(Void.class)) {
                    output = method.getReturnType();
                } else {
                    // ?
                }
                Serializer serializer = new Serializer(method, input, output);
                serializers.add(serializer);
            }
            return serializers;
        }
        
        public static Serializer find(Iterable<Serializer> serializers,
                Class<?> inputType, Class<?> outputType) {
            Serializer bestMatch = null;
            for (Serializer serializer: serializers) {
                if (serializer.inputType().isAssignableFrom(inputType)
                        && outputType.isAssignableFrom(serializer.outputType())) {
                    if (bestMatch == null 
                            || ! serializer.inputType().isAssignableFrom(bestMatch.inputType())
                            || ! serializer.outputType().isAssignableFrom(bestMatch.outputType())) {
                        bestMatch = serializer;
                    }
                }
            }
            return bestMatch;
        }
        
        protected final Method method;
        protected final Class<?> inputType;
        protected final Class<?> outputType;
        
        public Serializer(
                Method method,
                Class<?> inputType, 
                Class<?> outputType) {
            this.method = method;
            this.inputType = inputType;
            this.outputType = outputType;
        }
        
        public Method method() {
            return method;
        }
        
        public Class<?> inputType() {
            return inputType;
        }

        public Class<?> outputType() {
            return outputType;
        }
    }
}
