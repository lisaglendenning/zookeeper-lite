package edu.uw.zookeeper.data;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Functions;


public class DefaultsZNodeLabelTrie<E extends DefaultsZNodeLabelTrie.DefaultsNode<E>> extends SynchronizedZNodeLabelTrie<E> {
    
    public static <E extends DefaultsNode<E>> DefaultsZNodeLabelTrie<E> of(E root) {
        return new DefaultsZNodeLabelTrie<E>(root);
    }

    public static interface DefaultsNode<E extends DefaultsNode<E>> extends ZNodeLabelTrie.Node<E> {

        E putIfAbsent(ZNodeLabel.Component label);
    }
    
    public static abstract class AbstractDefaultsNode<E extends AbstractDefaultsNode<E>> extends SynchronizedZNodeLabelTrie.SynchronizedNode<E> implements DefaultsNode<E> {

        protected AbstractDefaultsNode(
                ZNodeLabel.Path path,
                Pointer<? extends E> parent,
                Map<ZNodeLabel.Component, E> children) {
            super(path, parent, children);
        }
        
        @Override
        public synchronized E putIfAbsent(ZNodeLabel.Component label) {
            E child;
            if (delegate().containsKey(label)) {
                child = delegate().get(label);
            } else {
                child = newChild(label);
                delegate().put(label, child);
            }
            return child;
        }

        protected abstract E newChild(ZNodeLabel.Component label);
    }
    
    protected DefaultsZNodeLabelTrie(E root) {
        super(root);
    }
    
    public E putIfAbsent(ZNodeLabel label) {
        return putIfAbsent(label, Functions.<E>identity());
    }

    public <V> V putIfAbsent(ZNodeLabel label, Function<? super E, ? extends V> visitor) {
        return putIfAbsent(label, root(), visitor);
    }

    protected <V> V putIfAbsent(ZNodeLabel label, E node, Function<? super E, ? extends V> visitor) {
        synchronized (node) {
            ZNodeLabel.Component next;
            ZNodeLabel rest;
            if (label instanceof ZNodeLabel.Path) {
                ZNodeLabel.Path path = (ZNodeLabel.Path) label;
                if (path.isAbsolute()) {
                    return putIfAbsent(path.suffix(0), node, visitor);
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
            E child = node.putIfAbsent(next);
            return putIfAbsent(rest, child, visitor);
        }
    }
}
