package edu.uw.zookeeper.data;


import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import edu.uw.zookeeper.data.NameTrie.Pointer;


public class OptionalNode<V> extends DefaultsNode.AbstractDefaultsNode<OptionalNode<V>> implements Supplier<V> {

    public static <V> OptionalNode<V> root() {
        return root(Optional.<V>absent());
    }
    
    public static <V> OptionalNode<V> root(Optional<V> value) {
        return new OptionalNode<V>(checkNotNull(value), SimpleLabelTrie.<OptionalNode<V>>rootPointer());
    }

    public static <V> OptionalNode<V> child(ZNodeName name, OptionalNode<V> parent) {
        return child(Optional.<V>absent(), name, parent);
    }

    public static <V> OptionalNode<V> child(Optional<V> value, ZNodeName name, OptionalNode<V> parent) {
        return new OptionalNode<V>(checkNotNull(value), SimpleLabelTrie.weakPointer(name, parent));
    }

    private Optional<V> value;

    protected OptionalNode(
            Optional<V> value,
            Pointer<? extends OptionalNode<V>> parent) {
        super(parent);
        this.value = value;
    }
    
    protected OptionalNode(
            Optional<V> value,
            Pointer<? extends OptionalNode<V>> parent,
            Map<ZNodeName, OptionalNode<V>> children) {
        super(parent, children);
        this.value = value;
    }
    
    protected OptionalNode(
            Optional<V> value,
            ZNodePath path,
            Pointer<? extends OptionalNode<V>> parent,
            Map<ZNodeName, OptionalNode<V>> children) {
        super(path, parent, children);
        this.value = value;
    }
    
    public boolean isPresent() {
        return value.isPresent();
    }

    @Override
    public V get() {
        return value.get();
    }
    
    public void set(V value) {
        this.value = Optional.of(value);
    }
    
    public void unset() {
        this.value = Optional.absent();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper("")
                .add("path", path())
                .add("children", keySet())
                .add("value", get())
                .toString();
    }

    @Override
    protected OptionalNode<V> newChild(ZNodeName label) {
        return child(label, this);
    }
}
