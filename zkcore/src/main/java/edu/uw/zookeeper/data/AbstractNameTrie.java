package edu.uw.zookeeper.data;

import java.lang.ref.WeakReference;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;

import edu.uw.zookeeper.common.AbstractPair;


public abstract class AbstractNameTrie<E extends NameTrie.Node<E>> implements NameTrie<E> {
    
    public static ZNodePath toPath(Object obj) {
        ZNodePath path;
        if (obj instanceof ZNodePath) {
            path = (ZNodePath) obj;
        } else {
            path = ZNodePath.fromString(obj.toString());
        }
        return path;
    }
    
    public static <E extends Node<E>> ParentIterator<E> parentIterator(Pointer<? extends E> child) {
        return ParentIterator.from(child);
    }

    public static <E extends Node<E>> ZNodePath pathOf(Pointer<? extends E> pointer) {
        E parent = pointer.get();
        if (parent == null) {
            return RootZNodePath.getInstance();
        } else {
            return parent.path().join(pointer.name());
        }
    }

    public static <E extends Node<E>> StrongPointer<E> rootPointer() {
        return StrongPointer.from(EmptyZNodeLabel.getInstance(), null);
    }
    
    public static <E extends Node<E>> StrongPointer<E> strongPointer(ZNodeName label, E node) {
        return StrongPointer.from(label, node);
    }
    
    public static <E extends Node<E>> WeakPointer<E> weakPointer(ZNodeName label, E node) {
        return WeakPointer.from(label, node);
    }

    public static <E extends Node<E>> PreOrderTraversal<E> preOrder(E node) {
        return new PreOrderTraversal<E>(node);
    }

    public static <E extends Node<E>> BreadthFirstTraversal<E> breadthFirst(E node) {
        return new BreadthFirstTraversal<E>(node);
    }
    
    public static class ParentIterator<E extends Node<E>> extends UnmodifiableIterator<Pointer<? extends E>> {
    
        public static <E extends Node<E>> ParentIterator<E> from(Pointer<? extends E> child) {
            return new ParentIterator<E>(StrongPointer.from(child.name(), child.get()));
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
            next = StrongPointer.from(last.get().parent().name(), last.get().parent().get());
            return last;
        }
    }

    public static class StrongPointer<E extends Node<E>> extends AbstractPair<ZNodeName, E> implements Pointer<E> {

        public static <E extends Node<E>> StrongPointer<E> from(ZNodeName label, E node) {
            return new StrongPointer<E>(label, node);
        }
        
        public StrongPointer(ZNodeName label, E node) {
            super(label, node);
        }
        
        public ZNodeName name() {
            return first;
        }
        
        public E get() {
            return second;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .addValue(name())
                    .toString();
        }
    }

    public static class WeakPointer<E extends Node<E>> extends AbstractPair<ZNodeName, WeakReference<E>> implements Pointer<E> {

        public static <E extends Node<E>> WeakPointer<E> from(ZNodeName label, E node) {
            return new WeakPointer<E>(label, node);
        }
        
        public WeakPointer(ZNodeName label, E node) {
            super(label, new WeakReference<E>(node));
        }
        
        public ZNodeName name() {
            return first;
        }
        
        public E get() {
            return second.get();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .addValue(name())
                    .toString();
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
            throw new UnsupportedOperationException();
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("strategy", strategy()).add("last", last).add("pending", pending).toString();
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

    public static abstract class SimpleNode<E extends SimpleNode<E>> extends ForwardingMap<ZNodeName, E> implements Node<E> {

        public static ZNodeName toName(Object obj) {
            ZNodeName name;
            if (obj instanceof ZNodeName) {
                name = (ZNodeName) obj;
            } else {
                name = ZNodeName.fromString(obj.toString());
            }
            return name;
        }
        
        private final Pointer<? extends E> parent;
        private final Map<ZNodeName, E> children;
        private final ZNodePath path;

        protected SimpleNode(
                Pointer<? extends E> parent) {
            this(parent, Maps.<ZNodeName, E>newHashMap());
        }
        
        protected SimpleNode(
                Pointer<? extends E> parent,
                Map<ZNodeName, E> children) {
            this(pathOf(parent), parent, children);
        }
        
        protected SimpleNode(
                ZNodePath path,
                Pointer<? extends E> parent,
                Map<ZNodeName, E> children) {
            this.parent = parent;
            this.children = children;
            this.path = path;
        }
        
        public boolean remove() {
            E parent = parent().get();
            if (parent != null) {
                if (this == parent.get(parent().name())) {
                    E child = parent.remove(parent().name());
                    assert(child == this);
                    return true;
                }
            }
            return false;
        }

        @Override
        public ZNodePath path() {
            return path;
        }

        @Override
        public Pointer<? extends E> parent() {
            return parent;
        }
        
        @Override
        public boolean containsKey(Object key) {
            return children.containsKey(toName(key));
        }

        @Override
        public E get(Object k) {
            return children.get(toName(k));
        }

        @Override
        public E remove(Object k) {
            return children.remove(toName(k));
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("path", path())
                    .add("children", keySet())
                    .toString();
        }

        @Override
        protected Map<ZNodeName, E> delegate() {
            return children;
        }
    }
    
    private final E root;
    
    protected AbstractNameTrie(E root) {
        this.root = root;
    }
    
    public E root() {
        return root;
    }

    @Override
    public void clear() {
        // assume we use weak pointers for parent pointers
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
        return Objects.equal(v, item);
    }

    @Override
    public Set<Map.Entry<ZNodePath, E>> entrySet() {
        ImmutableSet.Builder<Map.Entry<ZNodePath, E>> entries = ImmutableSet.builder();
        for (E e: this) {
            entries.add(new AbstractMap.SimpleImmutableEntry<ZNodePath, E>(e.path(), e));
        }
        return entries.build();
    }

    @Override
    public boolean isEmpty() {
        return root.isEmpty();
    }

    @Override
    public Set<ZNodePath> keySet() {
        ImmutableSet.Builder<ZNodePath> keys = ImmutableSet.builder();
        for (E e: this) {
            keys.add(e.path());
        }
        return keys.build();
    }

    @Override
    public void putAll(Map<? extends ZNodePath, ? extends E> m) {
        for (Map.Entry<? extends ZNodePath, ? extends E> e: m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
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
    public Iterator<E> iterator() {
        return preOrder(root);
    }

    @Override
    public String toString() {
        return Iterables.toString(this);
    }
}
