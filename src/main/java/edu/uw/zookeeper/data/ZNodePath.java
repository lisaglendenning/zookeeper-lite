package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

public class ZNodePath implements CharSequence {
    // TODO: check if unicode is handled correctly

    public static final Character SLASH = '/';
    public static final ZNodePath ROOT = new ZNodePath(SLASH.toString());
    
    public static class ZNodeName implements CharSequence {
        public static final Set<String> ILLEGAL = Sets.newHashSet();
        static {
            ILLEGAL.add(".");
            ILLEGAL.add("..");
        }
        public static final ZNodeName RESERVED = new ZNodeName("zookeeper");

        public static ZNodeName create(String name) {
            return new ZNodeName(validate(name));
        }
        
        public static String validate(String name) {
            checkArgument(name != null);
            checkArgument(name.length() > 0);
            checkArgument(! ILLEGAL.contains(name));
            checkArgument(name.indexOf(SLASH) == -1);
            // TODO: check for illegal unicode values
            return name;
        }

        private final String name;

        private ZNodeName(String name) {
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
        public String toString() {
            return name;
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
    }
    
    public static ZNodePath create(String path) {
        path = canonicalize(path);
        if (path.length() == 1) {
            return ROOT;
        }
        return new ZNodePath(path);
    }

    public static ZNodePath join(String...names) {
        if (names.length > 0) {
            StringBuilder builder = new StringBuilder(SLASH.toString());
            for (String name: names) {
                builder.append(SLASH);
                builder.append(name);
            }
            return ZNodePath.create(builder.toString());
        } else {
            return ROOT;
        }
    }

    public static ZNodePath join(ZNodeName...names) {
        if (names.length > 0) {
            StringBuilder builder = new StringBuilder(SLASH.toString());
            for (ZNodeName name: names) {
                builder.append(SLASH);
                builder.append(name.toString());
            }
            return ZNodePath.create(builder.toString());
        } else {
            return ROOT;
        }
    }

    public static String validate(String path) {
        checkArgument(path != null);
        checkArgument(path.length() > 0);
        checkArgument(path.charAt(0) == SLASH);
        if (path.length() == 1) {
            return path;
        }
        Splitter splitter = Splitter.on(SLASH);
        for (String name : splitter.split(path.substring(1))) {
            ZNodeName.validate(name);
        }
        return path;
    }

    public static String canonicalize(String path) {
        path = validate(path);
        // don't end in a slash
        if ((path.length() > 1) && (path.charAt(path.length() - 1) == SLASH)) {
            path = path.substring(0, path.length() - 2);
        }
        return path;
    }

    private final String path;

    private ZNodePath(String path) {
        this.path = path;
    }

    public Optional<ZNodePath> parent() {
        // TODO: memoize since we are immutable
        if (ROOT.equals(this)) {
            return Optional.<ZNodePath> absent();
        }
        int lastSlash = path.lastIndexOf(SLASH);
        // we are canonicalized
        assert (lastSlash > 0) && (lastSlash < length() - 2) : path;
        return Optional.of(new ZNodePath(path.substring(0, lastSlash)));
    }

    public Optional<ZNodeName> name() {
        // TODO: memoize since we are immutable
        if (ROOT.equals(this)) {
            return Optional.<ZNodeName> absent();
        }
        int lastSlash = path.lastIndexOf(SLASH);
        // we are canonicalized
        assert (lastSlash > 0) && (lastSlash < length() - 2) : path;
        return Optional.of(new ZNodeName(path.substring(lastSlash + 1)));
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
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ZNodePath other = (ZNodePath) obj;
        return (Objects.equal(toString(), other.toString()));
    }

    @Override
    public String toString() {
        return path;
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
}
