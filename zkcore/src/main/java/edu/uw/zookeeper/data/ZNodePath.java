package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;

import com.google.common.base.Functions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

import edu.uw.zookeeper.common.Reference;

public abstract class ZNodePath extends ZNodeLabel implements Iterable<ZNodePathComponent> {

    /**
     * @param path to be validated
     * @return path
     */
    public static String validate(String path) {
        checkArgument(path != null, "null");
        int length = path.length();
        checkArgument(length > 0, "empty path");
        int lastSlash = -1;
        for (int i=0; i<length; ++i) {
            char c = path.charAt(i);
            if ((c == SLASH) || (i == length - 1)) {
                if (i > 0) {
                    String substr;
                    if (c == SLASH) {
                        checkArgument(i < length - 1, "trailing slash");
                        substr = path.substring(lastSlash + 1, i);
                    } else {
                        substr = path.substring(lastSlash + 1, i+1);
                    }
                    ZNodePathComponent component = ZNodePathComponent.validated(substr);
                    checkArgument(! (ZNodePathComponent.self().equals(component) 
                            || ZNodePathComponent.parent().equals(component)), component);
                }
                if (c == SLASH) {
                    lastSlash = i;
                }
            }
        }
        checkArgument(lastSlash >= 0, "not a path");
        return path;
    }

    /**
     * Validates, requires absolute, replaces self and parent components, and trims slashes.
     * 
     * @param path to be canonicalized
     * @return canonicalized path
     */
    public static String canonicalize(String path) {
        checkArgument(path != null, "null");
        int length = path.length();
        checkArgument(length > 0, "empty path");
        int lastSlash = -1;
        StringBuilder builder = null;
        for (int i=0; i<length; ++i) {
            char c = path.charAt(i);
            if ((c == SLASH) || (i == length - 1)) {
                if (i > 0) {
                    // neighboring slash?
                    if ((c == SLASH) && (lastSlash == i-1)) {
                        if (builder == null) {
                            builder = new StringBuilder(length)
                                .append(path.substring(0, lastSlash+1));
                        }
                    } else {
                        ZNodePathComponent component = ZNodePathComponent.validated(
                                path.substring(lastSlash+1, (c == SLASH) ? i : i+1));
                        if (ZNodePathComponent.self().equals(component)) {
                            checkArgument(lastSlash >= 0, "self can't be the first component");
                            if (builder == null) {
                                builder = new StringBuilder(length)
                                    .append(path.substring(0, lastSlash+1));
                            }
                        } else if (ZNodePathComponent.parent().equals(component)) {
                            checkArgument(lastSlash >= 0, "parent can't be the first component");
                            if (builder == null) {
                                builder = new StringBuilder(length)
                                    .append(path.substring(0, lastSlash+1));
                            }
                            int buildLength = builder.length();
                            assert(builder.charAt(buildLength -1) == SLASH);
                            int parentSlash = -1;
                            for (int j = buildLength-2; j >= 0; j--) {
                                if (builder.charAt(j) == SLASH) {
                                    parentSlash = j;
                                    break;
                                }
                            }
                            checkArgument(parentSlash >= 0, "no parent");
                            builder.delete(parentSlash + 1, buildLength);
                        } else {
                            if (builder != null) {
                                builder.append(path.substring(lastSlash+1, i+1));
                            }
                        }
                    }
                }
                if (c == SLASH) {
                    lastSlash = i;
                }
            }
        }
        String canonicalized;
        if (builder == null) {
            canonicalized = ((path.length() < 2) || (path.charAt(path.length() - 1) != SLASH)) ? path : path.substring(0, path.length() - 1);
        } else {
            // trailing slash?
            int buildLength = builder.length();
            if ((buildLength > 1) && (builder.charAt(buildLength - 1) == SLASH)) {
                builder.deleteCharAt(buildLength - 1);
            }
            canonicalized = builder.toString();
        }
        checkArgument(! canonicalized.isEmpty(), "empty path");
        checkArgument(canonicalized.charAt(0) == SLASH, "not absolute path");
        return canonicalized;
    }

    public static String join(String...components) {
        return join(Iterators.forArray(components));
    }

    public static String join(Iterator<String> components) {
        StringBuilder builder = new StringBuilder();
        while (components.hasNext()) {
            String component = components.next();
            if (component == null) {
                continue;
            }
            if (builder.length() == 0) {
                builder.append(component);
            } else if (! component.isEmpty()) {
                if (component.charAt(0) == SLASH) {
                    if (component.length() == 1) {
                        continue;
                    }
                    if (SLASH == builder.charAt(builder.length() - 1)) {
                        component = component.substring(1);
                    }
                } else {
                    if (SLASH != builder.charAt(builder.length() - 1)) {
                        builder.append(SLASH);
                    }
                }
                builder.append(component);
            }
        }
        return builder.toString();
    }

    public static ZNodeLabel joined(Object...components) {
        return joined(Iterators.forArray(components));
    }

    public static ZNodeLabel joined(Iterator<?> components) {
        return of(join(Iterators.transform(components, Functions.toStringFunction())));
    }
    
    @Serializes(from=String.class, to=ZNodePath.class)
    public static ZNodePath of(String path) {
        if (path.charAt(0) == SLASH) {
            if (path.length() == 1) {
                return root();
            } else {
                return new AbsoluteZNodePath(path);
            }
        } else {
            return new RelativeZNodePath(path);
        }
    }

    public static ZNodePath validated(String path) {
        return of(validate(path));
    }
    
    public static ZNodePath canonicalized(String path) {
        return of(canonicalize(path));
    }

    public static RootZNodePath root() {
        return RootZNodePath.getInstance();
    }

    public static AbsoluteZNodePath zookeeper() {
        return AbsoluteZNodePath.Reserved.ZOOKEEPER.get();
    }
    
    public static String headOf(String path) {
        int lastSlash = path.lastIndexOf(ZNodeLabel.SLASH);
        if (lastSlash == -1) {
            return "";
        } else if (lastSlash == 0) {
            return (path.length() > 1) ? "/" : "";
        } else {
            return path.substring(0, lastSlash);
        }
    }

    public static String tailOf(String path) {
        int lastSlash = path.lastIndexOf(ZNodeLabel.SLASH);
        if ((lastSlash == -1) || (lastSlash == path.length() - 1)) {
            return "";
        } else {
            return path.substring(lastSlash + 1);
        }
    }

    private static final Splitter SPLITTER = Splitter.on(SLASH);

    private ZNodePath(String label) {
        super(label);
    }

    public abstract boolean isAbsolute();

    public abstract boolean isRoot();
    
    @Override
    public boolean startsWith(ZNodeLabel prefix) {
        int prefixLength = prefix.length();
        return toString().startsWith(prefix.toString()) && ((length() == prefixLength) || (charAt(prefixLength) == SLASH));
    }

    public ZNodeLabel head() {
        // TODO: memoize?
        int lastSlash = toString().lastIndexOf(SLASH);
        if (lastSlash == 0) {
            return root();
        }
        String head = toString().substring(0, lastSlash);
        if (head.indexOf(SLASH) >= 0) {
            return ZNodePath.of(head);
        } else {
            return ZNodePathComponent.of(head);
        }
    }

    public ZNodeLabel tail() {
        // TODO: memoize?
        int lastSlash = toString().lastIndexOf(SLASH);
        String tail = toString().substring(lastSlash + 1);
        return ZNodePathComponent.of(tail);
    }

    public ZNodeLabel prefix(int index) {
        int length = length();
        if ((index < 0) || (index > length)) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        if (index == 0) {
            return ZNodeLabel.none();
        } else if (index == length) {
            return this;
        } else {
            checkArgument(charAt(index) == SLASH, String.format("Cannot split prefix for %s at index %d", this, index));
            return ZNodeLabel.of(toString().substring(0, index));
        }
    }

    public ZNodeLabel suffix(int index) {
        int length = length();
        if ((index < 0) || (index > length)) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        if (index == length) {
            return ZNodeLabel.none();
        } else {
            checkArgument(charAt(index) == SLASH, String.format("Cannot split suffix for %s at index %d", this, index));
            return ZNodeLabel.of(toString().substring(index + 1));
        }
    }

    @Override
    public Iterator<ZNodePathComponent> iterator() {
        // TODO: memoize?
        return Iterators.transform(
                SPLITTER.omitEmptyStrings().split(toString()).iterator(),
                ZNodePathComponent.StringToComponent.OF);
    }

    public static class AbsoluteZNodePath extends ZNodePath {
        
        private AbsoluteZNodePath(String label) {
            super(label);
        }

        @Override
        public boolean isAbsolute() {
            return true;
        }

        @Override
        public boolean isRoot() {
            return false;
        }

        public static enum Reserved implements Reference<AbsoluteZNodePath> {
            ZOOKEEPER((AbsoluteZNodePath) ZNodePath.joined(root(), ZNodePathComponent.zookeeper())),
            QUOTA((AbsoluteZNodePath) ZNodePath.joined(ZOOKEEPER.get(), ZNodePathComponent.of("quota"))),
            CONFIG((AbsoluteZNodePath) ZNodePath.joined(ZOOKEEPER.get(), ZNodePathComponent.of("config")));
        
            private final AbsoluteZNodePath value;
            
            private Reserved(AbsoluteZNodePath value) {
                this.value = value;
            }
            
            @Override
            public AbsoluteZNodePath get() {
                return value;
            }
        }
    }
    
    public static final class RootZNodePath extends AbsoluteZNodePath {
        
        public static RootZNodePath getInstance() {
            return Reserved.ROOT.get();
        }
        
        private RootZNodePath() {
            super(Character.toString(SLASH));
        }
        
        @Override
        public boolean isRoot() {
            return true;
        }

        @Override
        public ZNodeLabel head() {
            return none();
        }

        @Override
        public ZNodeLabel tail() {
            return none();
        }

        @Override
        public UnmodifiableIterator<ZNodePathComponent> iterator() {
            return Iterators.emptyIterator();
        }
        
        public static enum Reserved implements Reference<RootZNodePath> {
            ROOT(new RootZNodePath());
        
            private final RootZNodePath value;
            
            private Reserved(RootZNodePath value) {
                this.value = value;
            }
            
            @Override
            public RootZNodePath get() {
                return value;
            }
        }
    }

    public static final class RelativeZNodePath extends ZNodePath {
        
        private RelativeZNodePath(String label) {
            super(label);
        }

        @Override
        public boolean isRoot() {
            return false;
        }
        
        @Override
        public boolean isAbsolute() {
            return false;
        }
    }
}