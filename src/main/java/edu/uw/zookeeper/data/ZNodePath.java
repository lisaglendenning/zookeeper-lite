package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;


import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;

public class ZNodePath implements CharSequence {

    public static final Character SLASH = '/';
    public static final ZNodePath ROOT = new ZNodePath(SLASH.toString());
    
    private static final Joiner joiner = Joiner.on(SLASH);
    private static final Splitter splitter = Splitter.on(SLASH);

    public static ZNodePath of(String path) {
        String canonicalized = canonicalize(path);
        if (canonicalized.length() == 1) {
            return ROOT;
        } else {
            return new ZNodePath(path);
        }
    }

    // TODO: strip intermediate slashes
    public static ZNodePath join(Object...names) {
        if (names.length > 0) {
            String path = joiner.appendTo(new StringBuilder(ROOT.toString()), names).toString();
            return ZNodePath.of(path);
        } else {
            return ROOT;
        }
    }

    public static String validate(String path) {
        checkArgument(path != null);
        checkArgument(path.length() > 0);
        boolean first = true;
        for (String name : splitter.split(path)) {
            if (first) {
                first = false;
                if (SLASH.equals(path.charAt(0))) {
                    assert (name.length() == 0);
                    continue;
                }
            }
            ZNodePathComponent.validate(name);
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

    private final String path;

    private ZNodePath(String path) {
        this.path = path;
    }
    
    public boolean isRoot() {
        return equals(ROOT);
    }
    
    public boolean isAbsolute() {
        return SLASH.equals(charAt(0));
    }

    public Optional<ZNodePath> parent() {
        // TODO: memoize since we are immutable
        if (isRoot()) {
            return Optional.<ZNodePath> absent();
        }
        int lastSlash = path.lastIndexOf(SLASH);
        // we are canonicalized
        assert (lastSlash > 0) && (lastSlash < length() - 2) : path;
        return Optional.of(ZNodePath.of(path.substring(0, lastSlash)));
    }

    public Optional<ZNodePathComponent> name() {
        // TODO: memoize since we are immutable
        if (isRoot()) {
            return Optional.<ZNodePathComponent> absent();
        }
        int lastSlash = path.lastIndexOf(SLASH);
        // we are canonicalized
        assert (lastSlash > 0) && (lastSlash < length() - 2) : path;
        return Optional.of(ZNodePathComponent.of(path.substring(lastSlash + 1)));
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
    public int hashCode() {
        return path.hashCode();
    }
}
