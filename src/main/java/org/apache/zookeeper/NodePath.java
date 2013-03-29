package org.apache.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

public class NodePath {
    // TODO: check if unicode is handled correctly

    public static final Character SLASH = '/';
    public static final NodePath ROOT = new NodePath(SLASH.toString());

    public static class NodeName {
        public static final Set<String> ILLEGAL = Sets.newHashSet();
        static {
            ILLEGAL.add(".");
            ILLEGAL.add("..");
        }
        public static final NodeName RESERVED = new NodeName("zookeeper");

        protected final String name;

        public static void validate(String name) {
            checkArgument(name != null);
            checkArgument(name.length() > 0);
            checkArgument(!(ILLEGAL.contains(name)));
            // TODO: check for illegal unicode values
        }

        public NodeName(String name) {
            validate(name);
            this.name = name;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            String other = null;
            if (obj instanceof String) {
                other = (String) obj;
            } else if (obj instanceof NodeName) {
                other = ((NodeName) obj).name;
            } else {
                return false;
            }
            return (Objects.equal(name, other));
        }

        @Override
        public String toString() {
            return name;
        }

        public int length() {
            return name.length();
        }
    }

    protected final String path;

    public static void validate(String path) {
        checkArgument(path != null);
        checkArgument(path.length() > 0);
        checkArgument(path.charAt(0) == SLASH);
        if (path.length() == 1) {
            return;
        }
        Splitter splitter = Splitter.on(SLASH);
        for (String name : splitter.split(path.substring(1))) {
            NodeName.validate(name);
        }
    }

    public static String canonicalize(String path) {
        validate(path);
        // don't end in a slash
        if ((path.length() > 1) && (path.charAt(path.length() - 1) == SLASH)) {
            path = path.substring(0, path.length() - 2);
        }
        return path;
    }

    public NodePath(String path) {
        path = canonicalize(path);
        this.path = path;
    }

    public Optional<NodePath> getParent() {
        // TODO: memoize since we are immutable
        if (ROOT.equals(this)) {
            return Optional.<NodePath> absent();
        }
        int lastSlash = path.lastIndexOf(SLASH);
        // we are canonicalized
        assert (lastSlash > 0) && (lastSlash < length() - 2) : path;
        return Optional.of(new NodePath(path.substring(0, lastSlash)));
    }

    public Optional<NodeName> getName() {
        // TODO: memoize since we are immutable
        if (ROOT.equals(this)) {
            return Optional.<NodeName> absent();
        }
        int lastSlash = path.lastIndexOf(SLASH);
        // we are canonicalized
        assert (lastSlash > 0) && (lastSlash < length() - 2) : path;
        return Optional.of(new NodeName(path.substring(lastSlash + 1)));
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        String other = null;
        if (obj instanceof String) {
            other = (String) obj;
        } else if (obj instanceof NodePath) {
            other = ((NodePath) obj).path;
        } else {
            return false;
        }
        return (Objects.equal(path, other));
    }

    @Override
    public String toString() {
        return path;
    }

    public int length() {
        return path.length();
    }
}
