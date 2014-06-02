package edu.uw.zookeeper.data;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


public enum Serializers {
    REGISTRY;
    
    public static Serializers getInstance() {
        return REGISTRY;
    }
    
    public static interface ByteSerializer<T> {
        byte[] toBytes(T input) throws IOException;
    }

    public static interface ByteDeserializer<T> {
        <U extends T> U fromBytes(byte[] bytes, Class<U> type) throws IOException;
    }
    
    public static interface ByteCodec<T> extends ByteSerializer<T>, ByteDeserializer<T> {}
    
    public <I,O> O toClass(I input, Class<O> outputType) {
        Class<?> inputType = input.getClass();
        Serializer method = find(inputType, outputType);
        if (method == null) {
            throw new IllegalArgumentException(String.format("Unable to serialize %s to %s", input, outputType));
        }
        O output = method.invoke(outputType, input);
        return output;
    }
    
    public static enum ToString implements Function<Object, String> {
        TO_STRING;
        
        public static ToString getInstance() {
            return TO_STRING;
        }
        
        @Override
        public String apply(Object input) {
            Class<?> inputType = input.getClass();
            Serializer serializer = Serializers.getInstance().find(inputType, String.class);
            if (serializer != null) {
                return serializer.invoke(input);
            } else {
                return String.valueOf(input);
            }
        }
    }
    
    public static final Set<Class<?>> PRIMITIVE_TYPES = ImmutableSet.<Class<?>>of(String.class, byte[].class);
    
    protected final ConcurrentMap<Class<?>, List<Serializer>> registry;
    protected final ConcurrentMap<Class<?>, Set<Serializer>> byType;
    
    private Serializers() {
        this.registry = Maps.newConcurrentMap();
        this.byType = Maps.newConcurrentMap();
    }
    
    public List<Serializer> add(Class<?> type) {
        List<Serializer> serializers = registry.get(type);
        if (serializers == null) {
            serializers = Serializer.discover(type);
            registry.put(type, serializers);
            
            if (! serializers.isEmpty()) {
                for (Serializer e: serializers) {
                    Class<?>[] types = { e.inputType(), e.outputType() };
                    for (Class<?> t: types) {
                        if (PRIMITIVE_TYPES.contains(t)) {
                            continue;
                        }
                        Set<Serializer> byTypeRegistry = byType.get(t);
                        if (byTypeRegistry == null) {
                            byType.putIfAbsent(t, Collections.synchronizedSet(Sets.<Serializer>newHashSet()));
                            byTypeRegistry = byType.get(t);
                        }
                        byTypeRegistry.add(e);
                    }
                }
            }
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
    
    public Serializer find(Class<?> inputType, Class<?> outputType) {
        Serializer serializer = null;
        Class<?>[] types = { inputType, outputType };
        for (Class<?> t: types) {
            if (PRIMITIVE_TYPES.contains(t)) {
                continue;
            }
            serializer = find(t, inputType, outputType);
            if (serializer != null) {
                break;
            }
        }
        if (serializer == null) {
            for (Class<?> t: types) {
                if (PRIMITIVE_TYPES.contains(t)) {
                    continue;
                }
                if (byType.containsKey(t)) {
                    serializer = Serializer.find(byType.get(t), inputType, outputType);
                    if (serializer != null) {
                        break;
                    }
                }
            }
        }
        return serializer;
    }
    
    public static class Serializer {

        public static List<Serializer> discover(Class<?> type) {
            List<Serializer> serializers = Lists.newLinkedList();
            for (Method method: type.getDeclaredMethods()) {
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
                    }
                }
                if (input.equals(Void.class)) {
                    throw new IllegalArgumentException(String.format("Unable to determine input type from %s", method));
                }
                Class<?> output = annotation.to();
                if (output.equals(Void.class)) {
                    output = method.getReturnType();
                }
                if (output.equals(Void.class)) {
                    throw new IllegalArgumentException(String.format("Unable to determine output type from %s", method));
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
        
        @SuppressWarnings("unchecked")
        public <O> O invoke(Object obj, Object...inputs) {
            Object invObj = obj;
            Object[] invInputs = inputs;
            if (Modifier.isStatic(method().getModifiers()) && (! (obj instanceof Class<?>))) {
                invObj = obj.getClass();
                invInputs = new Object[inputs.length + 1];
                invInputs[0] = obj;
                for (int i=0; i<inputs.length; ++i) {
                    invInputs[i+1] = inputs[i];
                }
            }
            O output;
            try {
                output = (O) method().invoke(invObj, invInputs);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Unable to invoke %s on %s %s", method(), invObj, Arrays.toString(invInputs)), e);
            }
            return output;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("method", method())
                    .add("inputType", inputType())
                    .add("outputType", outputType())
                    .toString();
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (! (obj instanceof Serializer)) {
                return false;
            }
            Serializer other = (Serializer)obj;
            return Objects.equal(method(), other.method())
                    && Objects.equal(inputType(), other.outputType())
                    && Objects.equal(outputType(), other.outputType());
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(method(), inputType(), outputType());
        }
    }
}
