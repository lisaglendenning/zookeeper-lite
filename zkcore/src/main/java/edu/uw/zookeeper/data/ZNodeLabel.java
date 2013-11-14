package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;

import edu.uw.zookeeper.common.Reference;

//TODO: check if unicode is handled correctly
public abstract class ZNodeLabel implements CharSequence, Comparable<ZNodeLabel> {

    public static final char SLASH = '/';
    
    public static final String THIS_COMPONENT = ".";
    public static final String PARENT_COMPONENT = "..";
    
    @Serializes(from=String.class, to=ZNodeLabel.class)
    public static ZNodeLabel of(String label) {
        if (label.length() == 0) {
            return None.getInstance();
        } else if (label.indexOf(SLASH) >= 0) {
            return Path.of(label);
        } else {
            return Component.of(label);
        }
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
    
    public static None none() {
        return None.getInstance();
    }

    public static final class None extends ZNodeLabel {

        public static None getInstance() {
            return Holder.getInstance();
        }
        
        protected static enum Holder implements Reference<None> {
            NONE(new None());
            
            public static None getInstance() {
                return NONE.get();
            }
            
            private final None instance;
            
            private Holder(None instance) {
                this.instance = instance;
            }
            
            @Override
            public None get() {
                return instance;
            }
        }
        
        private None() {
            super("");
        }
    }
    
    public static final class Component extends ZNodeLabel {

        // illegal unicode values:
        // u0000,
        // u0001 - u001F and u007F - u009F,
        // ud800 - uF8FF and uFFF0 - uFFFF,
        // uXFFFE - uXFFFF (where X is a digit 1 - E), 
        // uF0000 - uFFFFF
        protected static final CharMatcher ILLEGAL_CHARACTERS = 
                CharMatcher.inRange('\u0000', '\u001f')
                    .and(CharMatcher.inRange('\u007f', '\u009F'))
                    .and(CharMatcher.inRange('\ud800', '\uf8ff'))
                    .and(CharMatcher.inRange('\ufff0', '\uffff'))
                    .precomputed();
        
        public static String validate(String label) {
            checkArgument(label != null, "null");
            checkArgument(! label.isEmpty(), label);
            for (int i=0; i<label.length(); ++i) {
                char c = label.charAt(i);
                checkArgument(SLASH != c, label);
                checkArgument(! ILLEGAL_CHARACTERS.matches(c), label);
            }
            return label;
        }

        @Serializes(from=String.class, to=ZNodeLabel.Component.class)
        public static Component of(String label) {
            return new Component(label);
        }
        
        public static Component validated(String label) {
            return new Component(validate(label));
        }
        
        public static Component zookeeper() {
            return Reserved.ZOOKEEPER.get();
        }

        public static Component self() {
            return Reserved.SELF.get();
        }

        public static Component parent() {
            return Reserved.PARENT.get();
        }
        
        public static boolean isReserved(Component component) {
            return Reserved.contains(component);
        }
        
        private Component(String label) {
            super(label);
        }
        
        public static enum Reserved implements Reference<Component> {
            ZOOKEEPER(Component.of("zookeeper")), 
            SELF(Component.of(".")),
            PARENT(Component.of(".."));
            
            public static boolean contains(Component c) {
                for (Reserved e: values()) {
                    if (e.get().equals(c)) {
                        return true;
                    }
                }
                return false;
            }
            
            private final Component value;
            
            private Reserved(Component value) {
                this.value = value;
            }
            
            @Override
            public Component get() {
                return value;
            }
        }

        public static enum StringToComponent implements Function<String, Component> {
            OF;
        
            @Override
            public Component apply(String input) {
                return Component.of(input);
            }
        }
    }
    
    public static final class Path extends ZNodeLabel implements Iterable<Component> {

        protected static final Joiner JOINER = Joiner.on(SLASH);
        protected static final Splitter SPLITTER = Splitter.on(SLASH);

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
                        Component component = Component.validated(substr);
                        checkArgument(! (Component.self().equals(component) 
                                || Component.parent().equals(component)), component);
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
                            Component component = Component.validated(
                                    path.substring(lastSlash+1, (c == SLASH) ? i : i+1));
                            if (Component.self().equals(component)) {
                                checkArgument(lastSlash >= 0, "self can't be the first component");
                                if (builder == null) {
                                    builder = new StringBuilder(length)
                                        .append(path.substring(0, lastSlash+1));
                                }
                            } else if (Component.parent().equals(component)) {
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

        public static Path root() {
            return Reserved.ROOT.get();
        }

        public static Path zookeeper() {
            return Reserved.ZOOKEEPER.get();
        }
        
        @Serializes(from=String.class, to=ZNodeLabel.Path.class)
        public static Path of(String path) {
            if ((path.length() == 1) && (path.charAt(0) == SLASH)) {
                return root();
            } else {
                return new Path(path);
            }
        }

        public static Path validated(String path) {
            return of(validate(path));
        }
        
        public static Path canonicalized(String path) {
            return of(canonicalize(path));
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

        private Path(String label) {
            super(label);
        }

        public boolean isAbsolute() {
            return (SLASH == charAt(0));
        }

        public boolean isRoot() {
            return isAbsolute() && (length() == 1);
        }

        public ZNodeLabel head() {
            // TODO: memoize?
            if (isRoot()) {
                return None.getInstance();
            }
            int lastSlash = toString().lastIndexOf(SLASH);
            if (lastSlash == 0) {
                return root();
            }
            String head = toString().substring(0, lastSlash);
            if (head.indexOf(SLASH) >= 0) {
                return Path.of(head);
            } else {
                return Component.of(head);
            }
        }

        public ZNodeLabel tail() {
            // TODO: memoize?
            if (isRoot()) {
                return None.getInstance();
            }
            int lastSlash = toString().lastIndexOf(SLASH);
            String tail = toString().substring(lastSlash + 1);
            return Component.of(tail);
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
                checkArgument(charAt(index) == SLASH, String.format("Cannot split %s at index %d", this, index));
                return ZNodeLabel.of(label.substring(0, index));
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
                checkArgument(charAt(index) == SLASH, String.format("Cannot split %s at index %d", this, index));
                return ZNodeLabel.of(label.substring(index + 1));
            }
        }
        
        public boolean prefixOf(ZNodeLabel.Path other) {
            checkState(this.isAbsolute());
            checkArgument(other.isAbsolute());
            
            if (isRoot()) {
                return true;
            }
            
            String myPath = toString();
            String otherPath = other.toString();
            if (! otherPath.startsWith(myPath)) {
                return false;
            }
            
            int length = myPath.length();
            if ((length == otherPath.length()) || (SLASH == otherPath.charAt(length))) {
                return true;
            } else {
                return false;
            }
        }
        
        @Override
        public Iterator<Component> iterator() {
            // TODO: memoize?
            return Iterators.transform(
                    SPLITTER.omitEmptyStrings().split(toString()).iterator(),
                    Component.StringToComponent.OF);
        }

        public static enum Reserved implements Reference<Path> {
            ROOT(new Path(Character.toString(SLASH))), 
            ZOOKEEPER((Path) Path.joined(ROOT.get(), Component.zookeeper())),
            QUOTA((Path) Path.joined(ZOOKEEPER.get(), Component.of("quota"))),
            CONFIG((Path) Path.joined(ZOOKEEPER.get(), Component.of("config")));
        
            private final Path value;
            
            private Reserved(Path value) {
                this.value = value;
            }
            
            @Override
            public Path get() {
                return value;
            }
        }
    }
    
    protected final String label;

    protected ZNodeLabel(String label) {
        this.label = label;
    }

    @Override
    public int compareTo(ZNodeLabel other) {
        return toString().compareTo(other.toString());
    }
    
    @Override
    @Serializes(from=ZNodeLabel.class, to=String.class)
    public String toString() {
        return label;
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
        if (! (obj instanceof ZNodeLabel)) {
            return false;
        }
        ZNodeLabel other = (ZNodeLabel) obj;
        return (Objects.equal(toString(), other.toString()));
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
