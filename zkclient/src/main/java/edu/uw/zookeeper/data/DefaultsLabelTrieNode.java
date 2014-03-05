package edu.uw.zookeeper.data;

import java.util.Iterator;
import java.util.Map;

import edu.uw.zookeeper.data.ZNodePath.AbsoluteZNodePath;


public interface DefaultsLabelTrieNode<E extends DefaultsLabelTrieNode<E>> extends LabelTrie.Node<E> {
    
    E putIfAbsent(ZNodeLabel label);

    public static abstract class AbstractDefaultsNode<E extends AbstractDefaultsNode<E>> extends SimpleLabelTrie.SimpleNode<E> implements DefaultsLabelTrieNode<E> {

        public static <E extends DefaultsLabelTrieNode<E>> E putIfAbsent(LabelTrie<E> trie, AbsoluteZNodePath path) {
            Iterator<ZNodePathComponent> remaining = path.iterator();
            E node = trie.root();
            while (remaining.hasNext()) {
                node = node.putIfAbsent(remaining.next());
            }
            return node;
        }
        
        protected AbstractDefaultsNode(
                AbsoluteZNodePath path,
                LabelTrie.Pointer<? extends E> parent,
                Map<ZNodeLabel, E> children) {
            super(path, parent, children);
        }
        
        @Override
        public E putIfAbsent(ZNodeLabel label) {
            E child;
            if (delegate().containsKey(label)) {
                child = delegate().get(label);
            } else {
                child = newChild(label);
                delegate().put(label, child);
            }
            return child;
        }

        protected abstract E newChild(ZNodeLabel label);
    }
}
