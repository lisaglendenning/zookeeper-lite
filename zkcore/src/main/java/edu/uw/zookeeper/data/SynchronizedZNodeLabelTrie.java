package edu.uw.zookeeper.data;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


public class SynchronizedZNodeLabelTrie<E extends SynchronizedZNodeLabelTrie.Node<E>> extends ZNodeLabelTrie<E> {
    
    public static <E extends Node<E>> SynchronizedZNodeLabelTrie<E> of(E root) {
        return new SynchronizedZNodeLabelTrie<E>(root);
    }

    public static abstract class SynchronizedNode<E extends AbstractNode<E>> extends AbstractNode<E> {
        
        protected SynchronizedNode(
                ZNodeLabel.Path path,
                Pointer<? extends E> parent,
                Map<ZNodeLabel.Component, E> children) {
            super(path, parent, children);
        }

        @Override
        public synchronized boolean remove(ZNodeLabel key, E value) {
            E child = delegate().get(key);
            if (value.equals(child)) {
                synchronized (child) {
                    delegate().remove(key);
                }
                return true;
            } else {
                return false;
            }
        }
        
        @Override
        public synchronized boolean containsKey(Object key) {
            return super.containsKey(key);
        }

        @Override
        public synchronized boolean containsValue(Object value) {
            return delegate().containsValue(value);
        }
        
        @Override
        public synchronized void putAll(Map<? extends ZNodeLabel.Component,? extends E> m) {
            standardPutAll(m);
        }

        @Override
        public synchronized E get(Object k) {
            return super.get(k);
        }
        
        @Override
        public synchronized E put(ZNodeLabel.Component label, E node) {
            synchronized (node) {
                return delegate().put(label, node);
            }
        }

        @Override
        public synchronized E remove(Object k) {
            ZNodeLabel label = toLabel(k);
            E node = delegate().get(label);
            if (node != null) {
                synchronized (node) {
                    delegate().remove(label);
                }
            }
            return node;
        }
        
        @Override
        public synchronized int size() {
            return delegate().size();
        }
        
        @Override
        public synchronized boolean isEmpty() {
            return delegate().isEmpty();
        }
        
        @Override
        public synchronized void clear() {
            Iterator<Map.Entry<ZNodeLabel.Component, E>> entries = delegate().entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<ZNodeLabel.Component, E> entry = entries.next();
                synchronized (entry.getValue()) {
                    entries.remove();
                }
            }
        }
        
        @Override
        public synchronized Set<ZNodeLabel.Component> keySet() {
          return ImmutableSet.copyOf(delegate().keySet());
        }

        @Override
        public synchronized Collection<E> values() {
          return ImmutableList.copyOf(delegate().values());
        }

        @Override
        public synchronized Set<Map.Entry<ZNodeLabel.Component, E>> entrySet() {
          return ImmutableSet.copyOf(delegate().entrySet());
        }

        @Override 
        public synchronized int hashCode() {
            return delegate().hashCode();
        }
    }
    
    protected SynchronizedZNodeLabelTrie(E root) {
        super(root);
    }

    @Override
    protected <V> V longestPrefix(ZNodeLabel label, E node, Function<? super E, ? extends V> visitor) {
        synchronized (node) {
            return super.longestPrefix(label, node, visitor);
        }
    }
}
