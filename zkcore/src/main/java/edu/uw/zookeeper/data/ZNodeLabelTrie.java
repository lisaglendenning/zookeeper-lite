package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkState;

import java.lang.ref.WeakReference;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;

import edu.uw.zookeeper.common.AbstractPair;


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
    
    public static interface Pointer<E extends Node<E>> {
        ZNodeLabel label();
        E get();
    }
    
    public static class ParentIterator<E extends Node<E>> extends UnmodifiableIterator<Pointer<? extends E>> {

        public static <E extends Node<E>> ParentIterator<E> from(Pointer<? extends E> child) {
            return new ParentIterator<E>(strongPointer(child.label(), child.get()));
        }

        protected Pointer<? extends E> next;
        
        protected ParentIterator(Pointer<? extends E> next) {
            this.next = next;
        }
        
        @Override
        public boolean hasNext() {
            return (next.get() != null);
        }

        @Override
        public Pointer<? extends E> next() {
            if (next.get() == null) {
                throw new NoSuchElementException();
            }
            Pointer<? extends E> last = next;
            next = strongPointer(last.get().parent().label(), last.get().parent().get());
            return last;
        }
    }
    
    public static <E extends Node<E>> ParentIterator<E> parentIterator(Pointer<? extends E> child) {
        return ParentIterator.from(child);
    }

    public static <E extends Node<E>> ZNodeLabel.Path pathOf(Pointer<? extends E> pointer) {
        Deque<ZNodeLabel> components = Lists.newLinkedList();
        Iterator<Pointer<? extends E>> itr = parentIterator(pointer);
        while (itr.hasNext()) {
            Pointer<? extends E> next = itr.next();
            components.addFirst(next.label());
        }
        components.addFirst(ZNodeLabel.Path.root());
        return (ZNodeLabel.Path) ZNodeLabel.joined(components.iterator());
    }

    public static interface Node<E extends Node<E>> extends Map<ZNodeLabel.Component, E> {
        Pointer<? extends E> parent();

        ZNodeLabel.Path path();
        
        boolean remove();
    }

    public static <E extends Node<E>> StrongPointer<E> strongPointer(ZNodeLabel label, E node) {
        return new StrongPointer<E>(label, node);
    }
    
    public static class StrongPointer<E extends Node<E>> extends AbstractPair<ZNodeLabel, E> implements Pointer<E> {

        public StrongPointer(ZNodeLabel label, E node) {
            super(label, node);
        }
        
        public ZNodeLabel label() {
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

    public static <E extends Node<E>> WeakPointer<E> weakPointer(ZNodeLabel label, E node) {
        return new WeakPointer<E>(label, node);
    }

    public static class WeakPointer<E extends Node<E>> extends AbstractPair<ZNodeLabel, WeakReference<E>> implements Pointer<E> {

        public WeakPointer(ZNodeLabel label, E node) {
            super(label, new WeakReference<E>(node));
        }
        
        public ZNodeLabel label() {
            return first;
        }
        
        public E get() {
            return second.get();
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .addValue(label())
                    .toString();
        }
    }
    
    public static abstract class AbstractNode<E extends AbstractNode<E>> extends ForwardingMap<ZNodeLabel.Component, E> implements Node<E> {
        
        private final Pointer<? extends E> parent;
        private final Map<ZNodeLabel.Component, E> children;
        private final ZNodeLabel.Path path;
        
        protected AbstractNode(
                ZNodeLabel.Path path,
                Pointer<? extends E> parent,
                Map<ZNodeLabel.Component, E> children) {
            this.parent = parent;
            this.children = children;
            this.path = path;
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public boolean remove() {
            checkState(!path().isRoot());
            
            E parent = parent().get();
            if (parent != null) {
                return parent.remove(parent().label(), (E) this);
            } else {
                return true;
            }
        }

        public boolean remove(ZNodeLabel key, E value) {
            if (value.equals(children.get(key))) {
                children.remove(key);
                return true;
            } else {
                return false;
            }
        }
        
        @Override
        public ZNodeLabel.Path path() {
            return path;
        }

        @Override
        public Pointer<? extends E> parent() {
            return parent;
        }
        
        @Override
        public boolean containsKey(Object key) {
            return children.containsKey(toLabel(key));
        }

        @Override
        public E get(Object k) {
            return children.get(toLabel(k));
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

        @Override
        protected Map<ZNodeLabel.Component, E> delegate() {
            return children;
        }
    }
    
    public static enum TraversalStrategy {
        PREORDER, BREADTH_FIRST;
    }

    public static abstract class IterativeTraversal<E extends Node<E>> implements Iterator<E> {

        protected E last;
        protected final LinkedList<E> pending;
        
        protected IterativeTraversal(E root) {
            this.last = null;
            this.pending = Lists.newLinkedList();
            pending.add(root);
        }
        
        public abstract TraversalStrategy strategy();

        @Override
        public boolean hasNext() {
            return !pending.isEmpty();
        }

        @Override
        public E next() {
            last = dequeue();
            for (E child: childrenOf(last)) {
                enqueue(child);
            }
            return last;
        }
        
        @Override
        public void remove() {
            if (last == null) {
                throw new IllegalStateException();
            }
            last.remove();
            last = null;
        }
        
        protected Iterable<E> childrenOf(E node) {
            return node.values();
        }
        
        /**
         * @return next node
         * @throws NoSuchElementException
         */
        protected abstract E dequeue();
        
        protected abstract void enqueue(E node);
    }
    
    public static class PreOrderTraversal<E extends Node<E>> extends IterativeTraversal<E> {

        public PreOrderTraversal(E root) {
            super(root);
        }
        
        @Override
        public TraversalStrategy strategy() {
            return TraversalStrategy.PREORDER;
        }

        @Override
        protected E dequeue() {
            return pending.pop();
        }

        @Override
        protected void enqueue(E node) {
            pending.push(node);
        }
    }

    public static class BreadthFirstTraversal<E extends Node<E>> extends IterativeTraversal<E> {

        public BreadthFirstTraversal(E root) {
            super(root);
        }

        @Override
        public TraversalStrategy strategy() {
            return TraversalStrategy.BREADTH_FIRST;
        }

        @Override
        protected E dequeue() {
            return pending.remove();
        }

        @Override
        protected void enqueue(E node) {
            pending.add(node);
        }
    }
    
    public static class NullIfPathNotEqual<E extends Node<E>> implements Function<E,E> {
    
        public static <E extends Node<E>> NullIfPathNotEqual<E> forPath(ZNodeLabel.Path path) {
            return new NullIfPathNotEqual<E>(path);
        }
        
        private final ZNodeLabel.Path path;
        
        public NullIfPathNotEqual(ZNodeLabel.Path path) {
            this.path = path;
        }
        
        @Override
        public @Nullable E apply(E input) {
            if (input.path().equals(path)) {
                return input;
            } else {
                return null;
            }
        }
    }

    private final E root;
    
    protected ZNodeLabelTrie(E root) {
        this.root = root;
    }
    
    public E root() {
        return root;
    }

    public E longestPrefix(ZNodeLabel label) {
        return longestPrefix(label, Functions.<E>identity());
    }

    public <V> V longestPrefix(ZNodeLabel label, Function<? super E, ? extends V> visitor) {
        return longestPrefix(label, root(), visitor);
    }
    
    protected <V> V longestPrefix(ZNodeLabel label, E node, Function<? super E, ? extends V> visitor) {
        ZNodeLabel.Component next;
        ZNodeLabel rest;
        if (label instanceof ZNodeLabel.Path) {
            ZNodeLabel.Path path = (ZNodeLabel.Path) label;
            if (path.isAbsolute()) {
                return longestPrefix(path.suffix(0), node, visitor);
            } else {
                int index = path.toString().indexOf(ZNodeLabel.SLASH);
                next = (ZNodeLabel.Component) path.prefix(index);
                rest = path.suffix(index);
            }
        } else if (label instanceof ZNodeLabel.Component) {
            next = (ZNodeLabel.Component) label;
            rest = ZNodeLabel.none();
        } else {
            return visitor.apply(node);
        }
        E child = node.get(next);
        if (child == null) {
            return visitor.apply(node);
        } else {
            return longestPrefix(rest, child, visitor);
        }
    }
    
    @Override
    public void clear() {
        root.clear();
    }

    @Override
    public boolean containsKey(Object k) {
        return get(k) != null;
    }

    @Override
    public boolean containsValue(Object v) {
        @SuppressWarnings("unchecked")
        E item = get(((E) v).path());
        return v.equals(item);
    }

    @Override
    public Set<Map.Entry<ZNodeLabel.Path, E>> entrySet() {
        ImmutableSet.Builder<Map.Entry<ZNodeLabel.Path, E>> entries = ImmutableSet.builder();
        for (E e: this) {
            entries.add(new AbstractMap.SimpleImmutableEntry<ZNodeLabel.Path, E>(e.path(), e));
        }
        return entries.build();
    }

    @Override
    public E get(Object k) {
        return get(k, Functions.<E>identity());
    }

    public <V> V get(Object k, Function<? super E, ? extends V> visitor) {
        ZNodeLabel label = toLabel(k);
        if ((label instanceof ZNodeLabel.Path) && ((ZNodeLabel.Path) label).isAbsolute()) {
            ZNodeLabel.Path path = (ZNodeLabel.Path) label;
            return longestPrefix(path, Functions.compose(visitor, NullIfPathNotEqual.<E>forPath(path)));
        } else {
            return get(ZNodeLabel.joined(ZNodeLabel.Path.root(), label), visitor);
        }
    }

    @Override
    public boolean isEmpty() {
        return root.isEmpty();
    }

    @Override
    public Set<ZNodeLabel.Path> keySet() {
        ImmutableSet.Builder<ZNodeLabel.Path> keys = ImmutableSet.builder();
        for (E e: this) {
            keys.add(e.path());
        }
        return keys.build();
    }

    @Override
    public E put(final ZNodeLabel.Path k, final E v) {
        return get(k.head(), new Function<E,E>() {
            @Override
            public E apply(E input) {
                if (input == null) {
                    throw new IllegalStateException();
                }
                return input.put((ZNodeLabel.Component) k.tail(), v);
            }
        });
    }

    @Override
    public void putAll(Map<? extends ZNodeLabel.Path, ? extends E> m) {
        for (Map.Entry<? extends ZNodeLabel.Path, ? extends E> e: m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public E remove(Object k) {
        E e = get(k);
        if (e != null) {
            if (e.remove()) {
                return e;
            } else {
                return null;
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
        return ImmutableList.copyOf(this);
    }

    @Override
    public IterativeTraversal<E> iterator() {
        return iterator(TraversalStrategy.PREORDER);
    }

    public IterativeTraversal<E> iterator(TraversalStrategy strategy) {
        switch (strategy) {
        case PREORDER:
            return new PreOrderTraversal<E>(root);
        case BREADTH_FIRST:
            return new BreadthFirstTraversal<E>(root);
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper("")
                .addValue(Iterables.toString(this))
                .toString();
    }
}
