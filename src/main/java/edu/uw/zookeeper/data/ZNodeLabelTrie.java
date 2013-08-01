package edu.uw.zookeeper.data;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.uw.zookeeper.data.ZNodeLabel.Component;
import edu.uw.zookeeper.util.AbstractPair;
import edu.uw.zookeeper.util.Reference;


public class ZNodeLabelTrie<E extends ZNodeLabelTrie.Node<E>> implements Map<ZNodeLabel.Path, E>, Iterable<E> {
    
    public static <E extends Node<E>> ZNodeLabelTrie<E> of(E root) {
        return new ZNodeLabelTrie<E>(root);
    }
    
    public static ZNodeLabel toLabel(Object obj) {
        ZNodeLabel label;
        if (obj instanceof ZNodeLabel) {
            label = (ZNodeLabel) obj;
        } else if (obj instanceof String) {
            label = ZNodeLabel.of((String) obj);
        } else {
            throw new ClassCastException();
        }
        return label;
    }
    
    public static interface Pointer<E extends Node<E>> extends Reference<E> {
        ZNodeLabel.Component label();
    }
    
    public static class ParentPointerIterator<E extends Node<E>> extends AbstractIterator<Pointer<E>> {

        public static <E extends Node<E>> ParentPointerIterator<E> from(Optional<? extends Pointer<E>> child) {
            return new ParentPointerIterator<E>(child);
        }
        
        protected Optional<? extends Pointer<E>> next;
        
        public ParentPointerIterator(Optional<? extends Pointer<E>> child) {
            this.next = child;
        }
        
        @Override
        protected Pointer<E> computeNext() {
            Optional<? extends Pointer<E>> next = this.next;
            if (next.isPresent()) {
                this.next = next.get().get().parent();
                return next.get();
            } else {
                return endOfData();
            }
        }
    }
    
    public static <E extends Node<E>> ParentPointerIterator<E> parentPointerIterator(Optional<? extends Pointer<E>> child) {
        return ParentPointerIterator.from(child);
    }

    public static <E extends Node<E>> ZNodeLabel.Path pathOf(Optional<? extends Pointer<E>> pointer) {
        Deque<ZNodeLabel> components = Lists.newLinkedList();
        Iterator<Pointer<E>> itr = parentPointerIterator(pointer);
        while (itr.hasNext()) {
            Pointer<E> next = itr.next();
            components.addFirst(next.label());
        }
        components.addFirst(ZNodeLabel.Path.root());
        return ZNodeLabel.Path.of(components.iterator());
    }

    public static class ParentIterator<E extends Node<E>> extends AbstractIterator<E> {

        public static <E extends Node<E>> ParentIterator<E> from(E child) {
            return new ParentIterator<E>(child);
        }
        
        protected E next;
        
        public ParentIterator(E child) {
            this.next = child;
        }
        
        @Override
        protected E computeNext() {
            E next = this.next;
            if (next != null) {
                Optional<Pointer<E>> parent = next.parent();
                if (parent.isPresent()) {
                    this.next = parent.get().get();
                } else {
                    this.next = null;
                }
                return next;
            } else {
                return endOfData();
            }
        }
    }
    
    public static <E extends Node<E>> ParentIterator<E> parentIterator(E child) {
        return ParentIterator.from(child);
    }

    // TODO: add a bit that tells us whether the node has been deleted?
    public static interface Node<E extends Node<E>> extends Map<ZNodeLabel.Component, E> {
        Optional<Pointer<E>> parent();

        ZNodeLabel.Path path();
    }
    
    public static class SimplePointer<E extends Node<E>> extends AbstractPair<ZNodeLabel.Component, E> implements Pointer<E> {

        public static <E extends Node<E>> SimplePointer<E> of(ZNodeLabel.Component label, E node) {
            return new SimplePointer<E>(label, node);
        }
        
        protected SimplePointer(ZNodeLabel.Component label, E node) {
            super(label, node);
        }
        
        public ZNodeLabel.Component label() {
            return first;
        }
        
        public E get() {
            return second;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .addValue(label())
                    .toString();
        }
    }

    public static abstract class AbstractNode<E extends AbstractNode<E>> extends ForwardingMap<ZNodeLabel.Component, E> implements Node<E> {
        
        public static ZNodeLabel toLabel(Object obj) {
            ZNodeLabel label;
            if (obj instanceof ZNodeLabel) {
                label = (ZNodeLabel)obj;
            } else if (obj instanceof String) {
                label = ZNodeLabel.of((String)obj);
            } else {
                throw new ClassCastException();
            }
            return label;
        }
        
        protected final Optional<Pointer<E>> parent;
        protected final Map<ZNodeLabel.Component, E> children;
        protected final ZNodeLabel.Path path;
        
        protected AbstractNode(
                Optional<Pointer<E>> parent,
                Map<ZNodeLabel.Component, E> children) {
            this.parent = parent;
            this.children = children;
            // parents are immutable, so pre-compute path
            this.path = pathOf(parent);
        }
        
        public boolean remove() {
            if (parent().isPresent()) {
                E node = parent().orNull().get().remove(parent().orNull().label());
                return (node != null);
            } else {
                return false;
            }
        }
        
        @Override
        protected Map<ZNodeLabel.Component, E> delegate() {
            return children;
        }

        @Override
        public ZNodeLabel.Path path() {
            return path;
        }

        @Override
        public Optional<Pointer<E>> parent() {
            return parent;
        }
        
        @Override
        public boolean containsKey(Object key) {
            return children.containsKey(toLabel(key));
        }
        
        @Override
        public void putAll(Map<? extends ZNodeLabel.Component,? extends E> m) {
            for (Map.Entry<? extends ZNodeLabel.Component,? extends E> e: m.entrySet()) {
                put(e.getKey(), e.getValue());
            }
        }

        @Override
        public E get(Object k) {
            return children.get(toLabel(k));
        }
        
        @Override
        public E put(ZNodeLabel.Component label, E node) {
            return children.put(label, node);
        }

        @Override
        public E remove(Object k) {
            return children.remove(toLabel(k));
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("children", keySet())
                    .toString();
        }
    }

    public static abstract class DefaultsNode<E extends DefaultsNode<E>> extends AbstractNode<E> implements Node<E> {

        protected DefaultsNode(Optional<Pointer<E>> parent) {
            super(parent, new ConcurrentSkipListMap<ZNodeLabel.Component, E>());
        }

        @Override
        protected ConcurrentSkipListMap<ZNodeLabel.Component, E> delegate() {
            return (ConcurrentSkipListMap<Component, E>) children;
        }
        
        public E add(Object k) {
            ZNodeLabel label = toLabel(k);
            if (label instanceof ZNodeLabel.Path) {
                @SuppressWarnings("unchecked")
                E parent = (E) this;
                E next = parent;
                for (ZNodeLabel.Component e: (ZNodeLabel.Path) label) {
                    next = parent.get(e);
                    if (next == null) {
                        next = parent.add(e);
                    }
                    parent = next;
                }
                return next;
            } else {
                ZNodeLabel.Component component = (ZNodeLabel.Component) label;
                delegate().putIfAbsent(component, newChild(component));
                return get(k);
            }
        }
        
        public boolean remove(Object k, Object v) {
            return delegate().remove(k, v);
        }

        @Override
        public boolean remove() {
            if (parent().isPresent()) {
                return parent().orNull().get().remove(parent().orNull().label(), this);
            } else {
                return false;
            }
        }
        
        protected abstract E newChild(ZNodeLabel.Component label);
    }
    
    public static class SimpleNode extends DefaultsNode<SimpleNode> {

        public static SimpleNode root() {
            return new SimpleNode(Optional.<Pointer<SimpleNode>>absent());
        }

        protected SimpleNode(Optional<Pointer<SimpleNode>> parent) {
            super(parent);
        }

        @Override
        protected SimpleNode newChild(Component label) {
            Pointer<SimpleNode> pointer = SimplePointer.of(label, this);
            return new SimpleNode(Optional.of(pointer));
        }
    }

    public static class ValueNode<V> extends DefaultsNode<ValueNode<V>> implements Reference<V> {

        public static <V> ValueNode<V> root(Function<ZNodeLabel.Path, V> toValue) {
            Optional<Pointer<ValueNode<V>>> pointer = Optional.absent();
            V value = toValue.apply(pathOf(pointer));
            return new ValueNode<V>(pointer, toValue, value);
        }

        protected final Function<ZNodeLabel.Path, V> toValue;
        protected final V value;
        
        protected ValueNode(
                Optional<Pointer<ValueNode<V>>> parent, 
                Function<ZNodeLabel.Path, V> toValue, 
                V value) {
            super(parent);
            this.toValue = toValue;
            this.value = value;
        }
        
        @Override
        public V get() {
            return value;
        }
        
        public Function<ZNodeLabel.Path, V> toValue() {
            return toValue;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("children", children.keySet())
                    .add("value", get())
                    .toString();
        }

        @Override
        protected ValueNode<V> newChild(Component label) {
            Pointer<ValueNode<V>> childPointer = SimplePointer.of(label, this);
            Optional<Pointer<ValueNode<V>>> pointer = Optional.of(childPointer);
            V value = toValue().apply(pathOf(pointer));
            return new ValueNode<V>(pointer, toValue(), value);
        }
    }
    
    public static enum TraversalStrategy {
        PREORDER, BREADTH_FIRST;
    }

    public abstract static class IterativeTraversal<E extends Node<E>> extends AbstractIterator<E> {

        protected final LinkedList<E> pending;
        
        protected IterativeTraversal(E root) {
            this.pending = Lists.newLinkedList();
            pending.add(root);
        }
        
        public abstract TraversalStrategy strategy();

        @Override
        protected E computeNext() {
            E next = dequeue();
            if (next != null) {
                for (E child: childrenOf(next)) {
                    enqueue(child);
                }
                return next;
            } else {
                return endOfData();
            }
        }
        
        protected Iterable<E> childrenOf(E node) {
            return node.values();
        }
        
        protected abstract E dequeue();
        
        protected abstract void enqueue(E node);
    }
    
    public static class PreOrderTraversal<E extends Node<E>> extends IterativeTraversal<E> {

        public static <E extends Node<E>> PreOrderTraversal<E> from(E root) {
            return new PreOrderTraversal<E>(root);
        }
        
        public PreOrderTraversal(E root) {
            super(root);
        }
        
        @Override
        public TraversalStrategy strategy() {
            return TraversalStrategy.PREORDER;
        }

        @Override
        protected E dequeue() {
            if (! pending.isEmpty()) {
                return pending.pop();
            } else {
                return null;
            }
        }

        @Override
        protected void enqueue(E node) {
            pending.push(node);
        }
    }

    public static class BreadthFirstTraversal<E extends Node<E>> extends IterativeTraversal<E> {

        public static <E extends Node<E>> BreadthFirstTraversal<E> from(E root) {
            return new BreadthFirstTraversal<E>(root);
        }
        
        public BreadthFirstTraversal(E root) {
            super(root);
        }

        @Override
        public TraversalStrategy strategy() {
            return TraversalStrategy.BREADTH_FIRST;
        }

        @Override
        protected E dequeue() {
            return pending.poll();
        }

        @Override
        protected void enqueue(E node) {
            pending.add(node);
        }
    }
    
    protected final E root;
    
    protected ZNodeLabelTrie(E root) {
        this.root = root;
    }
    
    public E root() {
        return root;
    }

    public E longestPrefix(ZNodeLabel label) {
        E floor = root();
        if (label instanceof ZNodeLabel.Path) {
            for (ZNodeLabel.Component component: (ZNodeLabel.Path) label) {
                E next = floor.get(component);
                if (next == null) {
                    break;
                } else {
                    floor = next;
                }
            }
        } else {
            floor = floor.get(label);
        }
        assert (floor != null);
        return floor;
    }

    @Override
    public void clear() {
        root().clear();
    }

    @Override
    public boolean containsKey(Object k) {
        return get(k) != null;
    }

    @Override
    public boolean containsValue(Object v) {
        @SuppressWarnings("unchecked")
        E item = get(((E)v).path());
        return ((item != null) && item.equals(v));
    }

    @Override
    public Set<Map.Entry<ZNodeLabel.Path, E>> entrySet() {
        Set<Map.Entry<ZNodeLabel.Path, E>> entries = Sets.newHashSet();
        for (E e: this) {
            entries.add(new AbstractMap.SimpleImmutableEntry<ZNodeLabel.Path, E>(e.path(), e));
        }
        return entries;
    }

    @Override
    public E get(Object k) {
        ZNodeLabel label = toLabel(k);
        E floor = longestPrefix(label);
        if (label.equals(floor.path())) {
            return floor;
        } else {
            return null;
        }
    }

    @Override
    public boolean isEmpty() {
        return root().isEmpty();
    }

    @Override
    public Set<ZNodeLabel.Path> keySet() {
        Set<ZNodeLabel.Path> keys = Sets.newHashSet();
        for (E e: this) {
            keys.add(e.path());
        }
        return keys;
    }

    @Override
    public E put(ZNodeLabel.Path k, E v) {
        E parent = get(k.head());
        if (parent == null) {
            throw new IllegalStateException();
        }
        return parent.put((ZNodeLabel.Component) k.tail(), v);
    }

    @Override
    public void putAll(Map<? extends ZNodeLabel.Path, ? extends E> m) {
        for (Map.Entry<? extends ZNodeLabel.Path, ? extends E> e: m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public E remove(Object k) {
        ZNodeLabel label = toLabel(k);
        E e = get(label);
        if (e != null) {
            Pointer<E> parent = e.parent().orNull();
            if (parent != null) {
                parent.get().remove(parent.label());
            } else {
                throw new IllegalArgumentException(k.toString());
            }
        }
        return e;
    }

    @Override
    public int size() {
        return values().size();
    }

    @Override
    public Collection<E> values() {
        return Lists.newArrayList(this);
    }

    @Override
    public IterativeTraversal<E> iterator() {
        return iterator(TraversalStrategy.PREORDER);
    }

    public IterativeTraversal<E> iterator(TraversalStrategy strategy) {
        switch (strategy) {
        case PREORDER:
            return new PreOrderTraversal<E>(root());
        case BREADTH_FIRST:
            return new BreadthFirstTraversal<E>(root());
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper("")
                .addValue(Iterators.toString(iterator()))
                .toString();
    }
}
