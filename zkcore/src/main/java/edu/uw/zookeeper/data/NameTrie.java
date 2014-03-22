package edu.uw.zookeeper.data;

import java.util.Map;

import com.google.common.base.Supplier;


public interface NameTrie<E extends NameTrie.Node<E>> extends Map<ZNodePath, E>, Iterable<E> {
    
    public static interface Pointer<E extends Node<E>> extends Supplier<E> {
        ZNodeName name();
        E get();
    }
    
    public static interface Node<E extends Node<E>> extends Map<ZNodeName, E> {
        Pointer<? extends E> parent();
        ZNodePath path();
    }
    
    abstract public E root();
    
    abstract public E longestPrefix(ZNodePath path);
}
