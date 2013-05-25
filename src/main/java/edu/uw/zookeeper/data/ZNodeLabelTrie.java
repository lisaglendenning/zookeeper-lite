package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.Constructor;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.uw.zookeeper.util.AbstractPair;
import edu.uw.zookeeper.util.Reference;


public class ZNodeLabelTrie<E extends ZNodeLabelTrie.Node<E>> implements Map<ZNodeLabel.Path, E>, Iterable<E> {
    
    public static <E extends Node<E>> ZNodeLabelTrie<E> of(E root) {
        return new ZNodeLabelTrie<E>(root);
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
    public static interface Node<E extends Node<E>> {
        Optional<Pointer<E>> parent();

        ZNodeLabel.Path path();
        
        SortedMap<ZNodeLabel.Component, E> children();

        E get(String label);
        
        E get(ZNodeLabel.Component label);

        E put(String label);

        E put(ZNodeLabel.Component label);

        E remove(ZNodeLabel.Component label);
        
        boolean isEmpty();
        
        void clear(); 
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

    public static abstract class AbstractNode<E extends AbstractNode<E>> implements Node<E> {
        
        protected final Optional<Pointer<E>> parent;
        protected final ConcurrentNavigableMap<ZNodeLabel.Component, E> children;
        protected final ZNodeLabel.Path path;
        
        protected AbstractNode(Optional<Pointer<E>> parent) {
            this.parent = parent;
            this.children = new ConcurrentSkipListMap<ZNodeLabel.Component, E>();
            // parents are immutable, so pre-compute path
            this.path = pathOf(parent);
        }

        @Override
        public Optional<Pointer<E>> parent() {
            return parent;
        }

        @Override
        public SortedMap<ZNodeLabel.Component, E> children() {
            return Collections.unmodifiableSortedMap(children);
        }

        @Override
        public E get(String label) {
            return get(ZNodeLabel.Component.of(label));
        }
        
        @Override
        public E get(ZNodeLabel.Component label) {
            return children.get(label);
        }
        
        @Override
        public E put(String label) {
            return put(ZNodeLabel.Component.of(label));
        }
        
        @Override
        public E put(ZNodeLabel.Component label) {
            checkArgument(label != null);
            E child = children.get(label);
            if (child != null) {
                return child;
            }
            child = newChild(label);
            E prevChild = children.putIfAbsent(label, child);
            if (prevChild != null) {
                return prevChild;
            } else {
                return child;
            }
        }

        @Override
        public boolean isEmpty() {
            return children.isEmpty();
        }
        
        @SuppressWarnings("unchecked")
        protected E newChild(ZNodeLabel.Component label) {
            try {
                Pointer<E> pointer = SimplePointer.of(label, (E) this);
                Optional<Pointer<E>> parent = Optional.of(pointer);
                Constructor<E> constructor = (Constructor<E>) getClass().getConstructor(parent.getClass());
                return constructor.newInstance(parent);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public E remove(ZNodeLabel.Component label) {
            checkArgument(label != null);
            return children.remove(label);
        }

        @Override
        public void clear() {
            children.clear();
        }

        @Override
        public ZNodeLabel.Path path() {
            return path;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("children", children.keySet())
                    .toString();
        }
    }
    
    public static class SimpleNode extends AbstractNode<SimpleNode> {

        public static SimpleNode root() {
            return new SimpleNode(Optional.<Pointer<SimpleNode>>absent());
        }

        protected SimpleNode(Optional<Pointer<SimpleNode>> parent) {
            super(parent);
        }
        
        @Override
        protected SimpleNode newChild(ZNodeLabel.Component label) {
            Pointer<SimpleNode> childPointer = SimplePointer.of(label, this);
            return new SimpleNode(Optional.of(childPointer));
        }
    }

    public static class ValueNode<V> extends AbstractNode<ValueNode<V>> implements Reference<V> {

        public static <V> ValueNode<V> root(Function<ZNodeLabel.Path, V> values) {
            Optional<Pointer<ValueNode<V>>> pointer = Optional.absent();
            V value = values.apply(pathOf(pointer));
            return new ValueNode<V>(pointer, values, value);
        }
        
        protected final Function<ZNodeLabel.Path, V> values;
        protected final V value;
        
        protected ValueNode(
                Optional<Pointer<ValueNode<V>>> parent, 
                Function<ZNodeLabel.Path, V> values, 
                V value) {
            super(parent);
            this.values = values;
            this.value = value;
        }
        
        @Override
        public V get() {
            return value;
        }
        
        public Function<ZNodeLabel.Path, V> values() {
            return values;
        }

        @Override
        protected ValueNode<V> newChild(ZNodeLabel.Component label) {
            Pointer<ValueNode<V>> pointer = SimplePointer.of(label, this);
            Optional<Pointer<ValueNode<V>>> parent = Optional.of(pointer);
            V value = values().apply(pathOf(parent));
            return new ValueNode<V>(parent, values(), value);
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("children", children.keySet())
                    .add("value", get())
                    .toString();
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
            return node.children().values();
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

    public E longestPrefix(ZNodeLabel.Path path) {
        E floor = root();
        for (ZNodeLabel.Component component: path) {
            E next = floor.get(component);
            if (next == null) {
                break;
            } else {
                floor = next;
            }
        }
        assert (floor != null);
        return floor;
    }
    
    public E put(String path) {
        return put(ZNodeLabel.Path.of(path));
    }
    
    public E put(ZNodeLabel.Path path) {
        E parent = root();
        E next = parent;
        for (ZNodeLabel.Component component: path) {
            next = parent.get(component);
            if (next == null) {
                next = parent.put(component);
            }
            parent = next;
        }
        assert (next != null);
        return next;
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
        ZNodeLabel.Path path = (k instanceof String) 
                ? ZNodeLabel.Path.of((String)k) : (ZNodeLabel.Path)k;
        E floor = longestPrefix(path);
        if (path.equals(floor.path())) {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends ZNodeLabel.Path, ? extends E> arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E remove(Object k) {
        ZNodeLabel.Path path = (k instanceof String) 
                ? ZNodeLabel.Path.of((String)k) : (ZNodeLabel.Path)k;
        checkArgument(! path.isRoot());
        E e = get(path);
        if (e != null) {
            Pointer<E> parent = e.parent().orNull();
            parent.get().remove(parent.label());
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
        return Objects.toStringHelper(this)
                .addValue(Iterators.toString(iterator()))
                .toString();
    }
}
