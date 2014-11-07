package edu.uw.zookeeper.data;

import java.util.Map;

import javax.annotation.Nonnull;


public class SimpleNameTrie<E extends AbstractNameTrie.SimpleNode<E>> extends AbstractNameTrie<E> {
    
    public static <E extends AbstractNameTrie.SimpleNode<E>> SimpleNameTrie<E> forRoot(E root) {
        return new SimpleNameTrie<E>(root);
    }

    protected SimpleNameTrie(E root) {
        super(root);
    }

    @Override
    public @Nonnull E longestPrefix(ZNodePath path) {
        E node = root();
        ZNodeName remaining = path.suffix(node.path());
        while (!(remaining instanceof EmptyZNodeLabel)) {
            E next = node.get(remaining);
            if (next != null) {
                remaining = EmptyZNodeLabel.getInstance();
            } else if (remaining instanceof RelativeZNodePath) {
                RelativeZNodePath relative = (RelativeZNodePath) remaining;
                for (Map.Entry<ZNodeName, E> child: node.entrySet()) {
                    if (relative.startsWith(child.getKey())) {
                        next = child.getValue();
                        remaining = relative.suffix(child.getKey().length());
                        break;
                    }
                }
            }
            if (next != null) {
                node = next;
            } else {
                break;
            } 
        }
        return node;
    }
    
    @Override
    public E get(Object k) {
        ZNodePath path = toPath(k);
        E node = longestPrefix(path);
        if (node.path().length() == path.length()) {
            return node;
        } else {
            return null;
        }
    }

    @Override
    public E put(ZNodePath k, E v) {
        if (k.isRoot()) {
            throw new IllegalArgumentException(String.valueOf(k));
        }
        E parent = longestPrefix(k);
        if (parent.path().length() == k.length()) {
            parent = parent.parent().get();
        }
        return parent.put(k.suffix(parent.path().length()), v);
    }

    @Override
    public E remove(Object k) {
        ZNodePath path = toPath(k);
        if (path.isRoot()) {
            throw new IllegalArgumentException(String.valueOf(k));
        }
        E node = get(path);
        if ((node != null) && node.remove()) {
            return node;
        }
        return null;
    }
}
