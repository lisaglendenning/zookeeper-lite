package edu.uw.zookeeper.common;

import java.util.Iterator;



public abstract class Generators {
    
    public static <V> ConstantGenerator<V> constant(V value) {
        return ConstantGenerator.forValue(value);
    }

    public static <V> IteratorGenerator<V> iterator(Iterator<V> values) {
        return IteratorGenerator.forIterator(values);
    }

    public static <V> DereferencingGenerator<V> dereferencing(Generator<? extends Generator<? extends V>> generator) {
        return DereferencingGenerator.forGenerator(generator);
    }

    public static class ConstantGenerator<V> implements Generator<V> {

        public static <V> ConstantGenerator<V> forValue(V value) {
            return new ConstantGenerator<V>(value);
        }
        
        private final V value;
        
        public ConstantGenerator(V value) {
            this.value = value;
        }
        
        @Override
        public V next() {
            return value;
        }
    }
    
    public static class DereferencingGenerator<V> implements Generator<V> {

        public static <V> DereferencingGenerator<V> forGenerator(Generator<? extends Generator<? extends V>> generator) {
            return new DereferencingGenerator<V>(generator);
        }
        
        private final Generator<? extends Generator<? extends V>> generator;
        
        public DereferencingGenerator(Generator<? extends Generator<? extends V>> generator) {
            this.generator = generator;
        }
        
        @Override
        public V next() {
            return generator.next().next();
        }
    }

    public static class IteratorGenerator<V> implements Generator<V> {

        public static <V> IteratorGenerator<V> forIterator(Iterator<V> values) {
            return new IteratorGenerator<V>(values);
        }
        
        private final Iterator<V> values;

        protected IteratorGenerator(Iterator<V> values) {
            this.values = values;
        }
        
        @Override
        public V next() {
            return values.next();
        }
    }
    
    private Generators() {}
}
