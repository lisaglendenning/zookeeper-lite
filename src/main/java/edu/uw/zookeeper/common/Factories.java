package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

public abstract class Factories {

    public static <T,U> U apply(
            Factory<? extends T> head,
            ParameterizedFactory<? super T, ? extends U> tail) {
        return tail.get(head.get());
    }

    public static <T,U,V> U apply(V value,
            ParameterizedFactory<? super V, ? extends T> head,
            ParameterizedFactory<? super T, ? extends U> tail) {
        return tail.get(head.get(value));
    }

    public static <T> HolderFactory<T> holderOf(T delegate) {
        return HolderFactory.newInstance(delegate);
    }

    public static <T> HolderFactory<T> holderFrom(Factory<T> delegate) {
        return HolderFactory.newInstance(delegate.get());
    }

    public static <T> LazyHolder<T> lazyFrom(Factory<? extends T> factory) {
        return LazyHolder.newInstance(factory);
    }

    public static <T> SynchronizedLazyHolder<T> synchronizedLazyFrom(Factory<? extends T> factory) {
        return SynchronizedLazyHolder.newInstance(factory);
    }
    
    public static <T,U> LinkedFactory<T,U> link(
                Factory<? extends T> head,
                ParameterizedFactory<? super T, ? extends U> tail) {
        return LinkedFactory.newInstance(head, tail);
    }

    public static <V,T,U> LinkedParameterizedFactory<V,T,U> linkParameterized(
                ParameterizedFactory<? super V, ? extends T> head,
                ParameterizedFactory<? super T, ? extends U> tail) {
        return LinkedParameterizedFactory.newInstance(head, tail);
    }

    public static <V,T,U> LinkedDefaultsFactory<V,T,U> linkDefaults(
            DefaultsFactory<? super V, ? extends T> head,
            ParameterizedFactory<? super T, ? extends U> tail) {
        return LinkedDefaultsFactory.newInstance(head, tail);
    }
    
    public static class Holder<T> implements Reference<T> {
        
        public static <T> Holder<T> of(T instance) {
            return new Holder<T>(instance);
        }
        
        private final T instance;
        
        @Override
        public T get() {
            return instance;
        }
        
        public Holder(T instance) {
            this.instance = instance;
        }

        @Override
        public int hashCode() {
            return instance.hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (obj.getClass() != getClass())) {
                return false;
            }
            return Objects.equal(instance, ((Holder<?>) obj).instance);
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(instance).toString();
        }
    }
    
    public static class HolderFactory<T> implements Singleton<T> {

        public static <T> HolderFactory<T> newInstance(Factory<T> delegate) {
            return new HolderFactory<T>(delegate.get());
        }
        
        public static <T> HolderFactory<T> newInstance(T value) {
            return new HolderFactory<T>(value);
        }
        
        private final T value;
        
        protected HolderFactory(T value) {
            this.value = value;
        }
        
        @Override
        public T get() {
            return value;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(get()).toString();
        }
    }

    public static class LazyHolder<T> implements Singleton<T> {

        public static <T> LazyHolder<T> newInstance(Factory<? extends T> factory) {
            return new LazyHolder<T>(factory);
        }
        
        protected final Factory<? extends T> factory;
        protected Optional<T> instance;
        
        protected LazyHolder(Factory<? extends T> factory) {
            this.factory = factory;
            this.instance = Optional.<T>absent();
        }
        
        public boolean has() {
            return instance.isPresent();
        }
        
        /**
         * Not thread-safe!
         */
        @Override
        public T get() {
            if (! instance.isPresent()) {
                instance = Optional.<T>of(factory.get());
            }
            return instance.get();
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(instance).toString();
        }
    }
    
    public static class SynchronizedLazyHolder<T> extends LazyHolder<T> {

        public static <T> SynchronizedLazyHolder<T> newInstance(Factory<? extends T> factory) {
            return new SynchronizedLazyHolder<T>(factory);
        }
        
        protected SynchronizedLazyHolder(Factory<? extends T> factory) {
            super(factory);
        }

        @Override
        public synchronized boolean has() {
            return super.has();
        }
        
        @Override
        public synchronized T get() {
            return super.get();
        }

        @Override
        public synchronized String toString() {
            return super.toString();
        }
    }

    public abstract static class AbstractLinkedFactory<T,U,C> extends Pair<C, ParameterizedFactory<? super T, ? extends U>> {
        protected AbstractLinkedFactory(
                C head,
                ParameterizedFactory<? super T, ? extends U> tail) {
            super(head, tail);
        }
    }
    
    public static class LinkedFactory<T,U> extends AbstractLinkedFactory<T,U,Factory<? extends T>> implements Factory<U> {

        public static <T,U> LinkedFactory<T,U> newInstance(
                Factory<? extends T> head,
                ParameterizedFactory<? super T, ? extends U> tail) {
            return new LinkedFactory<T,U>(head, tail);
        }
        
        private LinkedFactory(
                Factory<? extends T> head,
                ParameterizedFactory<? super T, ? extends U> tail) {
            super(head, tail);
        }

        @Override
        public U get() {
            return apply(first(), second());
        }
    }
    
    public static class LinkedParameterizedFactory<V,T,U> extends AbstractLinkedFactory<T,U,ParameterizedFactory<? super V, ? extends T>> implements ParameterizedFactory<V,U> {

        public static <V,T,U> LinkedParameterizedFactory<V,T,U> newInstance(
                ParameterizedFactory<? super V, ? extends T> head,
                ParameterizedFactory<? super T, ? extends U> tail) {
            return new LinkedParameterizedFactory<V,T,U>(head, tail);
        }
        
        private LinkedParameterizedFactory(
                ParameterizedFactory<? super V, ? extends T> head,
                ParameterizedFactory<? super T, ? extends U> tail) {
            super(head, tail);
        }

        @Override
        public U get(V value) {
            return apply(value, first(), second());
        }
    }
    
    public static class LinkedDefaultsFactory<V,T,U> extends AbstractLinkedFactory<T,U,DefaultsFactory<? super V, ? extends T>> implements DefaultsFactory<V,U> {

        public static <V,T,U> LinkedDefaultsFactory<V,T,U> newInstance(
                DefaultsFactory<? super V, ? extends T> head,
                ParameterizedFactory<? super T, ? extends U> tail) {
            return new LinkedDefaultsFactory<V,T,U>(head, tail);
        }
        
        private LinkedDefaultsFactory(
                DefaultsFactory<? super V, ? extends T> head,
                ParameterizedFactory<? super T, ? extends U> tail) {
            super(head, tail);
        }

        @Override
        public U get() {
            return apply(first(), second());
        }

        @Override
        public U get(V value) {
            return apply(value, first(), second());
        }
    }
    
    public static class ByTypeFactory<T> implements ParameterizedFactory<Class<? extends T>, T> {

        public static <T> ByTypeFactory<T> newInstance(Map<Class<? extends T>, Factory<? extends T>> factories) {
            return new ByTypeFactory<T>(factories);
        }
        
        private final Map<Class<? extends T>, Factory<? extends T>> factories;
        
        protected ByTypeFactory(Map<Class<? extends T>, Factory<? extends T>> factories) {
            checkArgument(! factories.isEmpty());
            this.factories = ImmutableMap.copyOf(factories);
        }
        
        @Override
        public T get(Class<? extends T> type) {
            Factory<? extends T> factory = factories.get(type);
            if (factory != null) {
                return factory.get();
            } else {
                return null;
            }
        }
    }

    private Factories() {}
}
