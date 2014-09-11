package edu.uw.zookeeper.data;


import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.base.Supplier;

import edu.uw.zookeeper.data.NameTrie.Pointer;


public class ValueNode<V> extends AbstractNameTrie.SimpleNode<ValueNode<V>> implements Supplier<V> {
    
    public static <V> ValueNode<V> root(V value) {
        return new ValueNode<V>(value, AbstractNameTrie.<ValueNode<V>>rootPointer());
    }

    public static <V> ValueNode<V> child(V value, ZNodeName name, ValueNode<V> parent) {
        return new ValueNode<V>(value, AbstractNameTrie.weakPointer(name, parent));
    }

    private final V value;

    protected ValueNode(
            V value,
            Pointer<? extends ValueNode<V>> parent) {
        super(parent);
        this.value = value;
    }
    
    protected ValueNode(
            V value,
            Pointer<? extends ValueNode<V>> parent,
            Map<ZNodeName, ValueNode<V>> children) {
        super(parent, children);
        this.value = value;
    }
    
    protected ValueNode(
            V value,
            ZNodePath path,
            Pointer<? extends ValueNode<V>> parent,
            Map<ZNodeName, ValueNode<V>> children) {
        super(path, parent, children);
        this.value = value;
    }

    @Override
    public V get() {
        return value;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("")
                .add("path", path())
                .add("children", keySet())
                .add("value", get())
                .toString();
    }
}
