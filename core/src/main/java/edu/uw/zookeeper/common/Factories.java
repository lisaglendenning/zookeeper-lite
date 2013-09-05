package edu.uw.zookeeper.common;

import com.google.common.base.Objects;


public abstract class Factories {

    public static <T,U> Factory<U> applied(
            final ParameterizedFactory<? super T, ? extends U> factory,
            final T value) {
        return new Factory<U>() {
            @Override
            public U get() {
                return factory.get(value);
            }
        };
    }

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

    public static <T> SingletonHolder<T> singletonOf(T delegate) {
        return SingletonHolder.of(delegate);
    }

    public static <T> SingletonHolder<T> singletonFrom(Factory<T> delegate) {
        return SingletonHolder.of(delegate.get());
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
        
        protected final T instance;
        
        public Holder(T instance) {
            this.instance = instance;
        }

        @Override
        public T get() {
            return instance;
        }

        @Override
        public String toString() {
            return instance.toString();
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
    }
    
    public static class SingletonHolder<T> extends Holder<T> implements Singleton<T> {

        public static <T> SingletonHolder<T> of(T value) {
            return new SingletonHolder<T>(value);
        }
        
        public SingletonHolder(T value) {
            super(value);
        }
    }

    public static class LazyHolder<T> implements Reference<T> {

        public static <T> LazyHolder<T> newInstance(Factory<? extends T> factory) {
            return new LazyHolder<T>(factory);
        }
        
        protected final Factory<? extends T> factory;
        protected T instance;
        
        protected LazyHolder(Factory<? extends T> factory) {
            this.factory = factory;
            this.instance = null;
        }
        
        public boolean has() {
            return (instance != null);
        }
        
        /**
         * Not thread-safe!
         */
        @Override
        public T get() {
            if (instance == null) {
                instance = factory.get();
            }
            return instance;
        }

        @Override
        public String toString() {
            return String.valueOf(instance);
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
    private Factories() {}
}
