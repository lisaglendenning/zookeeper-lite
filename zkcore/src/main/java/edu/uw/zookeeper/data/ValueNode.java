package edu.uw.zookeeper.data;


import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;


public class ValueNode<V> extends SimpleNameTrie.SimpleNode<ValueNode<V>> implements Supplier<V> {
    
    public static <V> ValueNode<V> root(V value) {
        return new ValueNode<V>(SimpleNameTrie.<ValueNode<V>>rootPointer(), value);
    }

    public static <V> ValueNode<V> child(ZNodeName name, ValueNode<V> parent, V value) {
        NameTrie.Pointer<ValueNode<V>> childPointer = SimpleNameTrie.weakPointer(name, parent);
        return new ValueNode<V>(childPointer, value);
    }

    private final V value;
    
    protected ValueNode(NameTrie.Pointer<ValueNode<V>> parent, V value) {
        super(SimpleNameTrie.pathOf(parent), parent, Maps.<ZNodeName, ValueNode<V>>newHashMap());
        this.value = value;
    }

    @Override
    public V get() {
        return value;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper("")
                .add("path", path())
                .add("children", keySet())
                .add("value", get())
                .toString();
    }
}
