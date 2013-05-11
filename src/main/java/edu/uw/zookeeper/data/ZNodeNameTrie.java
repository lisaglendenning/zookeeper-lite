package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.util.AbstractPair;


public class ZNodeNameTrie {
    
    public static ZNodeNameTrie newInstance() {
        return new ZNodeNameTrie(Node.newInstance());
    }
    
    public static class Pointer extends AbstractPair<ZNodeName, Node> {

        public Pointer newInstance(ZNodeName label, Node node) {
            return new Pointer(label, node);
        }
        
        protected Pointer(ZNodeName label, Node node) {
            super(label, node);
        }
        
        public ZNodeName label() {
            return first;
        }
        
        public Node node() {
            return second;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .addValue(label())
                    .toString();
        }
    }
    
    public static class Node {
        
        public static Node newInstance() {
            return new Node(Optional.<Pointer>absent());
        }

        public static Node newInstance(Pointer parent) {
            return new Node(Optional.of(parent));
        }
        
        protected final Optional<Pointer> parent;
        protected final ConcurrentNavigableMap<ZNodeName, Node> children;
        protected final ZNodeName.Path path;
        
        protected Node(Optional<Pointer> parent) {
            this.parent = parent;
            this.children = new ConcurrentSkipListMap<ZNodeName, Node>();
            
            // parents are immutable, so pre-compute
            Optional<Pointer> prev = parent();
            if (!prev.isPresent()) {
                this.path = ZNodeName.Path.root();
            } else {
                List<ZNodeName> components = Lists.newLinkedList();
                while (prev.isPresent()) {
                    Pointer pointer = prev.get();
                    components.add(0, pointer.label());
                    prev = pointer.node().parent();
                }
                this.path = ZNodeName.Path.of(components);
            }
        }
        
        public Optional<Pointer> parent() {
            return parent;
        }
        
        public ConcurrentNavigableMap<ZNodeName, Node> children() {
            return children;
        }
        
        public Node add(Node child) {
            checkArgument(child != null);
            Optional<Pointer> pointer = child.parent();
            checkArgument(pointer.isPresent());
            checkArgument(this == pointer.get().node());
            return children().put(pointer.get().label(), child);
        }
        
        public ZNodeName.Path path() {
            return path;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("children", children())
                    .toString();
        }
    }
    
    protected final Node root;
    
    protected ZNodeNameTrie(Node root) {
        this.root = root;
    }
    
    public Node root() {
        return root;
    }
    
    public Node get(ZNodeName.Path path) {
        Node next = root();
        for (ZNodeName.Component component: path) {
            next = next.children().get(component);
            if (next == null) {
                break;
            }
        }        
        return next;
    }

    public ZNodeName.Path floorKey(ZNodeName.Path path) {
        Map.Entry<ZNodeName.Path, Node> entry = floorEntry(path);
        return (entry == null) ? null : entry.getKey();
    }
    
    public Map.Entry<ZNodeName.Path, Node> floorEntry(ZNodeName.Path path) {
        Node floor = root();
        for (ZNodeName.Component component: path) {
            Node next = floor.children().get(component);
            if (next != null) {
                floor = next;
            } else {
                break;
            }
        }
        return new AbstractMap.SimpleImmutableEntry<ZNodeName.Path, Node>(floor.path, floor);
    }
}
