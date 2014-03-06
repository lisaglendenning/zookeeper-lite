package edu.uw.zookeeper.data;

import java.util.Iterator;
import java.util.Map;


public interface DefaultsNode<E extends DefaultsNode<E>> extends NameTrie.Node<E> {
    
    E putIfAbsent(ZNodeName label);

    public static abstract class AbstractDefaultsNode<E extends AbstractDefaultsNode<E>> extends SimpleNameTrie.SimpleNode<E> implements DefaultsNode<E> {

        public static <E extends DefaultsNode<E>> E putIfAbsent(NameTrie<E> trie, ZNodePath path) {
            Iterator<ZNodeLabel> remaining = path.iterator();
            E node = trie.root();
            while (remaining.hasNext()) {
                node = node.putIfAbsent(remaining.next());
            }
            return node;
        }
        
        protected AbstractDefaultsNode(
                ZNodePath path,
                NameTrie.Pointer<? extends E> parent,
                Map<ZNodeName, E> children) {
            super(path, parent, children);
        }
        
        @Override
        public E putIfAbsent(ZNodeName label) {
            E child;
            if (delegate().containsKey(label)) {
                child = delegate().get(label);
            } else {
                child = newChild(label);
                delegate().put(label, child);
            }
            return child;
        }

        protected abstract E newChild(ZNodeName label);
    }
}
