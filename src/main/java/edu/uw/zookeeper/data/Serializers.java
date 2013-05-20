package edu.uw.zookeeper.data;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public enum Serializers {
    REGISTRY;
    
    public static Serializers getInstance() {
        return REGISTRY;
    }
    
    protected final ConcurrentMap<Class<?>, List<SerializerMethod>> registry;
    
    private Serializers() {
        this.registry = Maps.newConcurrentMap();
    }
    
    public List<SerializerMethod> add(Class<?> type) {
        List<SerializerMethod> serializers = registry.get(type);
        if (serializers == null) {
            serializers = SerializerMethod.discover(type);
            registry.put(type, serializers);
        }
        return serializers;
    }
    
    public List<SerializerMethod> get(Class<?> type) {
        return registry.get(type);
    }

    public SerializerMethod find(Class<?> type,
            Class<?> inputType, Class<?> outputType) {
        List<SerializerMethod> serializers = add(type);
        return SerializerMethod.find(serializers, inputType, outputType);
    }
    
    public static class SerializerMethod {

        public static List<SerializerMethod> discover(Class<?> type) {
            List<SerializerMethod> serializers = Lists.newLinkedList();
            for (Method method: type.getMethods()) {
                Serializer annotation = method.getAnnotation(Serializer.class);
                if (annotation == null) {
                    continue;
                }
                Class<?> input = annotation.input();
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
                Class<?> output = annotation.output();
                if (output.equals(Void.class)) {
                    output = method.getReturnType();
                } else {
                    // ?
                }
                SerializerMethod serializer = new SerializerMethod(method, annotation, input, output);
                serializers.add(serializer);
            }
            return serializers;
        }
        
        public static SerializerMethod find(Iterable<SerializerMethod> serializers,
                Class<?> inputType, Class<?> outputType) {
            SerializerMethod bestMatch = null;
            for (SerializerMethod serializer: serializers) {
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
        protected final Serializer annotation;
        protected final Class<?> inputType;
        protected final Class<?> outputType;
        
        public SerializerMethod(
                Method method,
                Serializer annotation,
                Class<?> inputType, 
                Class<?> outputType) {
            this.method = method;
            this.annotation = annotation;
            this.inputType = inputType;
            this.outputType = outputType;
        }
        
        public Method method() {
            return method;
        }
        
        public Serializer annotation() {
            return annotation;
        }
        
        public Class<?> inputType() {
            return inputType;
        }

        public Class<?> outputType() {
            return outputType;
        }
    }
}
