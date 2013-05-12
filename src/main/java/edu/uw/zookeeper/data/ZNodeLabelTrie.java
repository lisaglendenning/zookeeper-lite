package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.util.AbstractPair;


public class ZNodeLabelTrie {
    
    public static ZNodeLabelTrie newInstance() {
        return new ZNodeLabelTrie(Node.newInstance());
    }
    
    public static class Pointer extends AbstractPair<ZNodeLabel.Component, Node> {

        public Pointer newInstance(ZNodeLabel.Component label, Node node) {
            return new Pointer(label, node);
        }
        
        protected Pointer(ZNodeLabel.Component label, Node node) {
            super(label, node);
        }
        
        public ZNodeLabel.Component label() {
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
        protected final ConcurrentNavigableMap<ZNodeLabel.Component, Node> children;
        protected final ZNodeLabel.Path path;
        
        protected Node(Optional<Pointer> parent) {
            this.parent = parent;
            this.children = new ConcurrentSkipListMap<ZNodeLabel.Component, Node>();
            
            // parents are immutable, so pre-compute
            ZNodeLabel.Path path = ZNodeLabel.Path.root();
            Optional<Pointer> prev = parent();
            if (prev.isPresent()) {
                List<ZNodeLabel.Component> components = Lists.newLinkedList();
                while (prev.isPresent()) {
                    Pointer pointer = prev.get();
                    components.add(0, pointer.label());
                    prev = pointer.node().parent();
                }
                path = ZNodeLabel.Path.of(components.iterator());
            }
            this.path = path;
        }
        
        public Optional<Pointer> parent() {
            return parent;
        }
        
        public SortedMap<ZNodeLabel.Component, Node> children() {
            return Collections.unmodifiableSortedMap(children);
        }
        
        public Node add(Node child) {
            checkArgument(child != null);
            Optional<Pointer> pointer = child.parent();
            checkArgument(pointer.isPresent());
            checkArgument(this == pointer.get().node());
            return children.put(pointer.get().label(), child);
        }
        
        public ZNodeLabel.Path path() {
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
    
    protected ZNodeLabelTrie(Node root) {
        this.root = root;
    }
    
    public Node root() {
        return root;
    }
    
    public Node get(ZNodeLabel.Path path) {
        Node floor = longestPrefix(path);
        if (path.equals(floor.path())) {
            return floor;
        } else {
            return null;
        }
    }

    public Node longestPrefix(ZNodeLabel.Path path) {
        Node floor = root();
        if (path.isRoot()) {
            return floor;
        }
        for (ZNodeLabel.Component component: path) {
            Node next = floor.children().get(component);
            if (next == null) {
                break;
            } else {
                floor = next;
            }
        }
        assert (floor != null);
        return floor;
    }
}
