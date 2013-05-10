package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import edu.uw.zookeeper.util.Singleton;

//TODO: check if unicode is handled correctly
public abstract class ZNodeName implements CharSequence {

    public static final Character SLASH = '/';
    
    public static class Component extends ZNodeName {
        
        protected static final Set<String> ILLEGAL = ImmutableSet.of(".", "..");
        
        public static enum Reserved implements Singleton<Component> {
            ZOOKEEPER(new Component("zookeeper"));
            
            private final Component instance;
            
            private Reserved(Component instance) {
                this.instance = instance;
            }
            
            public Component get() {
                return instance;
            }
        }
        
        public static Component of(String name) {
            String validated = validate(name);
            return new Component(validated);
        }
        
        public static String validate(String name) {
            checkArgument(name != null);
            checkArgument(name.length() > 0);
            checkArgument(! ILLEGAL.contains(name));
            checkArgument(name.indexOf(SLASH) == -1);
            // TODO: check for illegal unicode values:
            // u0000,
            // u0001 - u001F and u007F - u009F,
            // ud800 - uF8FF and uFFF0 - uFFFF,
            // uXFFFE - uXFFFF (where X is a digit 1 - E), 
            // uF0000 - uFFFFF.
            return name;
        }

        private Component(String name) {
            super(name);
        }
        
        public boolean isReserved() {
            for (Reserved e: Reserved.values()) {
                if (equals(e.get())) {
                    return true;
                }
            }
            return false;
        }
    }
    
    public static class Path extends ZNodeName {

        public static Path root() {
            return Root.getInstance().get();
        }

        public static Path of(String path) {
            String canonicalized = canonicalize(path);
            Path root = root();
            if (root.toString().equals(canonicalized)) {
                return root;
            } else {
                return new Path(canonicalized);
            }
        }

        public static String validate(String path) {
            checkArgument(path != null);
            checkArgument(path.length() > 0);
            checkArgument(path.indexOf(SLASH) >= 0);
            if (root().toString().equals(path)) {
                return path;
            }
            boolean first = true;
            for (String name : splitter.split(path)) {
                // the only empty component should be the first one
                if (first) {
                    first = false;
                    if (SLASH.equals(path.charAt(0))) {
                        assert (name.length() == 0);
                        continue;
                    }
                }
                Component.validate(name);
            }
            return path;
        }

        public static String canonicalize(String path) {
            path = validate(path);
            // don't end in a slash
            int length = path.length();
            if ((length > 1) && SLASH.equals(path.charAt(length - 1))) {
                path = path.substring(0, length - 2);
            }
            return path;
        }

        public static enum Root implements Singleton<Path> {
            INSTANCE;
            
            public static Root getInstance() {
                return INSTANCE;
            }

            private final Path instance = new Path(SLASH.toString());
            
            @Override
            public Path get() {
                return instance;
            }
        }
        
        protected static final Joiner joiner = Joiner.on(SLASH);
        protected static final Splitter splitter = Splitter.on(SLASH);

        private Path(String path) {
            super(path);
        }

        public boolean isAbsolute() {
            return SLASH.equals(toString().charAt(0));
        }

        public boolean isRoot() {
            return equals(root());
        }

        public Path head() {
            // TODO: memoize since we are immutable
            if (isRoot()) {
                return null;
            }
            int lastSlash = toString().lastIndexOf(SLASH);
            // we are canonicalized
            assert (lastSlash > 0) && (lastSlash < length() - 2) : toString();
            return Path.of(toString().substring(0, lastSlash));
        }

        public Component tail() {
            // TODO: memoize since we are immutable
            if (isRoot()) {
                return null;
            }
            int lastSlash = toString().lastIndexOf(SLASH);
            // we are canonicalized
            assert (lastSlash > 0) && (lastSlash < length() - 2) : toString();
            return Component.of(toString().substring(lastSlash + 1));
        }

        public Path append(Component tail) {
            return append(tail.toString());
        }
        
        public Path append(Path tail) {
            return append(tail.toString());
        }
        
        public Path append(String tail) {
            if (SLASH.equals(tail.charAt(0))) {
                tail = tail.substring(1);
            }
            if (tail.length() == 0) {
                return this;
            } else {
                return Path.of(joiner.join(toString(), tail));
            }
        }
    }

    protected final String value;

    protected ZNodeName(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int length() {
        return toString().length();
    }

    @Override
    public char charAt(int arg0) {
        return toString().charAt(arg0);
    }

    @Override
    public CharSequence subSequence(int arg0, int arg1) {
        return toString().subSequence(arg0, arg1);
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
        ZNodeName other = (ZNodeName) obj;
        return (Objects.equal(toString(), other.toString()));
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
