package edu.uw.zookeeper.data;

import java.util.Iterator;

import javax.annotation.Nonnull;


/**
 * NameTrie in which every edge is a ZNodeLabel.
 * 
 * Not threadsafe.
 */
public class SimpleLabelTrie<E extends NameTrie.Node<E>> extends AbstractNameTrie<E> {
    
    public static <E extends Node<E>> SimpleLabelTrie<E> forRoot(E root) {
        return new SimpleLabelTrie<E>(root);
    }

    protected SimpleLabelTrie(E root) {
        super(root);
    }

    @Override
    public @Nonnull E longestPrefix(ZNodePath path) {
        Iterator<ZNodeLabel> remaining = path.iterator();
        E node = root();
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

    @Override
    public E get(Object k) {
        ZNodePath path = toPath(k);
        Iterator<? extends AbstractZNodeLabel> remaining = path.iterator();
        E node = root();
        while ((node != null) && remaining.hasNext()) {
            node = node.get(remaining.next());
        }
        return node;
    }

    @Override
    public E put(ZNodePath k, E v) {
        E parent = get(((AbsoluteZNodePath) k).parent());
        if (parent == null) {
            throw new IllegalStateException(k.toString());
        } else {
            return parent.put(v.parent().name(), v);
        }
    }

    @Override
    public E remove(Object k) {
        ZNodePath path = toPath(k);
        if (path.isRoot()) {
            throw new IllegalArgumentException(String.valueOf(k));
        }
        E parent = get(((AbsoluteZNodePath) path).parent());
        if (parent == null) {
            return null;
        }
        return parent.remove(((AbsoluteZNodePath) path).label());
    }
}
