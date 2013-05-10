package edu.uw.zookeeper.data;

import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.base.Objects;
import com.google.common.base.Optional;


public class ZNodeNameTrie {
    
    public static class Edge implements Comparable<Edge> {

        public Edge newInstance(ZNodeName.Component label, Node parent, Node child) {
            return new Edge(label, parent, child);
        }
        
        protected final ZNodeName.Component label;
        protected final Node parent;
        protected final Node child;
        
        protected Edge(ZNodeName.Component label, Node parent, Node child) {
            this.label = label;
            this.parent = parent;
            this.child = child;
        }
        
        public ZNodeName.Component label() {
            return label;
        }
        
        public Node parent() {
            return parent;
        }
        
        public Node child() {
            return child;
        }

        @Override
        public int compareTo(Edge other) {
            int value = label().compareTo(other.label());
            if (value == 0 && !equals(other)) {
                value = 1;
            }
            return value;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(label()).toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Edge other = (Edge) obj;
            return (Objects.equal(label(), other.label())
                    && Objects.equal(parent(), other.parent())
                    && Objects.equal(child(), other.child()));
        }

        @Override
        public int hashCode() {
            return label().hashCode();
        }
    }
    
    public static class Node {
        
        public static Node newInstance() {
            return new Node(Optional.<Edge>absent());
        }

        public static Node newInstance(Edge parent) {
            return new Node(Optional.of(parent));
        }
        
        protected final Optional<Edge> parent;
        protected final NavigableSet<Edge> children;
        
        protected Node(Optional<Edge> parent) {
            this.parent = parent;
            this.children = new ConcurrentSkipListSet<Edge>();
        }
        
        public Optional<Edge> parent() {
            return parent;
        }
        
        public NavigableSet<Edge> children() {
            return children;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("parent", parent())
                    .add("children", children())
                    .toString();
        }
    }
    
    protected final Node root;
    
    public ZNodeNameTrie() {
        this.root = Node.newInstance();
    }
    
    public Node root() {
        return root;
    }
}
