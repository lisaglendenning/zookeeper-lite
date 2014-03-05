package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkState;

import java.lang.ref.WeakReference;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.ZNodePath.AbsoluteZNodePath;


public class SimpleLabelTrie<E extends LabelTrie.Node<E>> implements LabelTrie<E> {
    
    public static <E extends Node<E>> SimpleLabelTrie<E> forRoot(E root) {
        return new SimpleLabelTrie<E>(root);
    }
    
    public static ZNodeLabel toLabel(Object obj) {
        ZNodeLabel label;
        if (obj instanceof ZNodeLabel) {
            label = (ZNodeLabel) obj;
        } else if (obj == null) {
            label = ZNodeLabel.none();
        } else {
            label = ZNodeLabel.of(obj.toString());
        }
        return label;
    }
    
    public static <E extends Node<E>> ParentIterator<E> parentIterator(Pointer<? extends E> child) {
        return ParentIterator.from(child);
    }

    public static <E extends Node<E>> AbsoluteZNodePath pathOf(Pointer<? extends E> pointer) {
        E parent = pointer.get();
        if (parent == null) {
            return ZNodePath.root();
        } else {
            return (AbsoluteZNodePath) ZNodePath.joined(parent.path(), pointer.label());
        }
    }

    public static <E extends Node<E>> StrongPointer<E> rootPointer() {
        return StrongPointer.from(ZNodeLabel.none(), null);
    }
    
    public static <E extends Node<E>> StrongPointer<E> strongPointer(ZNodeLabel label, E node) {
        return StrongPointer.from(label, node);
    }
    
    public static <E extends Node<E>> WeakPointer<E> weakPointer(ZNodeLabel label, E node) {
        return WeakPointer.from(label, node);
    }

    public static <E extends Node<E>> PreOrderTraversal<E> preOrder(E node) {
        return new PreOrderTraversal<E>(node);
    }

    public static <E extends Node<E>> BreadthFirstTraversal<E> breadthFirst(E node) {
        return new BreadthFirstTraversal<E>(node);
    }
    
    public static <E extends Node<E>> E longestPrefix(LabelTrie<E> trie, AbsoluteZNodePath path) {
        Iterator<ZNodePathComponent> remaining = path.iterator();
        E node = trie.root();
        while (remaining.hasNext()) {
            E next = node.get(remaining.next());
            if (next == null) {
                break;
            } else {
                node = next;
            }
        }
        return node;
    }

    public static class ParentIterator<E extends Node<E>> extends UnmodifiableIterator<Pointer<? extends E>> {
    
        public static <E extends Node<E>> ParentIterator<E> from(Pointer<? extends E> child) {
            return new ParentIterator<E>(StrongPointer.from(child.label(), child.get()));
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
            next = StrongPointer.from(last.get().parent().label(), last.get().parent().get());
            return last;
        }
    }

    public static class StrongPointer<E extends Node<E>> extends AbstractPair<ZNodeLabel, E> implements Pointer<E> {

        public static <E extends Node<E>> StrongPointer<E> from(ZNodeLabel label, E node) {
            return new StrongPointer<E>(label, node);
        }
        
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

    public static class WeakPointer<E extends Node<E>> extends AbstractPair<ZNodeLabel, WeakReference<E>> implements Pointer<E> {

        public static <E extends Node<E>> WeakPointer<E> from(ZNodeLabel label, E node) {
            return new WeakPointer<E>(label, node);
        }
        
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

    public static abstract class SimpleNode<E extends SimpleNode<E>> extends ForwardingMap<ZNodeLabel, E> implements Node<E> {
        
        private final Pointer<? extends E> parent;
        private final Map<ZNodeLabel, E> children;
        private final AbsoluteZNodePath path;
        
        protected SimpleNode(
                AbsoluteZNodePath path,
                Pointer<? extends E> parent,
                Map<ZNodeLabel, E> children) {
            this.parent = parent;
            this.children = children;
            this.path = path;
        }

        @Override
        public AbsoluteZNodePath path() {
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
        protected Map<ZNodeLabel, E> delegate() {
            return children;
        }
    }
    
    private final E root;
    
    protected SimpleLabelTrie(E root) {
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
        return v.equals(item);
    }

    @Override
    public Set<Map.Entry<AbsoluteZNodePath, E>> entrySet() {
        ImmutableSet.Builder<Map.Entry<AbsoluteZNodePath, E>> entries = ImmutableSet.builder();
        for (E e: this) {
            entries.add(new AbstractMap.SimpleImmutableEntry<AbsoluteZNodePath, E>(e.path(), e));
        }
        return entries.build();
    }

    @Override
    public E get(Object k) {
        ZNodeLabel label = toLabel(k);
        Iterator<ZNodePathComponent> remaining;
        if (label instanceof ZNodePath) { 
            remaining = ((ZNodePath) label).iterator();
        } else {
            remaining = Iterators.singletonIterator((ZNodePathComponent) label);
        }
        E node = root();
        while ((node != null) && remaining.hasNext()) {
            node = node.get(remaining.next());
        }
        return node;
    }

    @Override
    public boolean isEmpty() {
        return root.isEmpty();
    }

    @Override
    public Set<AbsoluteZNodePath> keySet() {
        ImmutableSet.Builder<AbsoluteZNodePath> keys = ImmutableSet.builder();
        for (E e: this) {
            keys.add(e.path());
        }
        return keys.build();
    }

    @Override
    public E put(AbsoluteZNodePath k, E v) {
        E parent = get(k.head());
        if (parent == null) {
            throw new IllegalStateException(k.toString());
        } else {
            return parent.put(v.parent().label(), v);
        }
    }

    @Override
    public void putAll(Map<? extends AbsoluteZNodePath, ? extends E> m) {
        for (Map.Entry<? extends AbsoluteZNodePath, ? extends E> e: m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public E remove(Object k) {
        E node = get(k);
        if (node != null) {
            E parent = node.parent().get();
            if (parent != null) {
                return parent.remove(node.parent().label());
            } else {
                checkState(!node.path().isRoot());
                return node;
            }
        } else {
            return null;
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
