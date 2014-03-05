package edu.uw.zookeeper.data;

import java.util.Map;
import com.google.common.base.Supplier;
import edu.uw.zookeeper.data.ZNodePath.AbsoluteZNodePath;


public interface LabelTrie<E extends LabelTrie.Node<E>> extends Map<AbsoluteZNodePath, E>, Iterable<E> {
    
    public static interface Pointer<E extends Node<E>> extends Supplier<E> {
        ZNodeLabel label();
        E get();
    }
    
    public static interface Node<E extends Node<E>> extends Map<ZNodeLabel, E> {
        Pointer<? extends E> parent();
        AbsoluteZNodePath path();
    }
    
    abstract public E root();
}
