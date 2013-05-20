package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import edu.uw.zookeeper.util.Singleton;

//TODO: check if unicode is handled correctly
public abstract class ZNodeLabel implements CharSequence, Comparable<ZNodeLabel> {

    public static final char SLASH = '/';
    
    @Serializer(input=String.class, output=ZNodeLabel.class)
    public static ZNodeLabel of(String label) {
        if (label.indexOf(SLASH) >= 0) {
            return Path.of(label);
        } else {
            return Component.of(label);
        }
    }
    
    public static class Component extends ZNodeLabel {

        protected static final Set<String> ILLEGAL_COMPONENTS = 
                ImmutableSet.of(".", "..");
        
        // illegal unicode values:
        // u0000,
        // u0001 - u001F and u007F - u009F,
        // ud800 - uF8FF and uFFF0 - uFFFF,
        // uXFFFE - uXFFFF (where X is a digit 1 - E), 
        // uF0000 - uFFFFF
        protected static final RangeSet<Character> ILLEGAL_CHARACTERS = 
                ImmutableRangeSet.<Character>builder()
                .add(Range.closed(Character.valueOf('\u0000'), Character.valueOf('\u001f')))
                .add(Range.closed(Character.valueOf('\u007f'), Character.valueOf('\u009F')))
                .add(Range.closed(Character.valueOf('\ud800'), Character.valueOf('\uf8ff')))
                .add(Range.closed(Character.valueOf('\ufff0'), Character.valueOf('\uffff')))
                .build();
        
        public static String validate(String label) {
            checkArgument(label != null, label);
            checkArgument(label.length() > 0, label);
            checkArgument(!ILLEGAL_COMPONENTS.contains(label), label);
            for (int i=0; i<label.length(); ++i) {
                Character c = label.charAt(i);
                checkArgument(SLASH != c);
                checkArgument(!ILLEGAL_CHARACTERS.contains(c), label);
            }
            return label;
        }

        @Serializer(input=String.class, output=ZNodeLabel.Component.class)
        public static Component of(String label) {
            String validated = validate(label);
            return new Component(validated);
        }
        
        public static enum Reserved implements Singleton<Component> {
            ZOOKEEPER(Component.of("zookeeper"));
            
            private final Component value;
            
            private Reserved(Component value) {
                this.value = value;
            }
            
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
        
        public static enum StringToString implements Function<String, String> {
            VALIDATE;

            @Override
            public String apply(String input) {
                return validate(input);
            }
        }

        protected Component(String label) {
            super(label);
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
    
    public static class Path extends ZNodeLabel implements Iterable<Component> {

        protected static final Joiner JOINER = Joiner.on(SLASH);
        protected static final Splitter SPLITTER = Splitter.on(SLASH);

        public static String validate(String path) {
            checkArgument(path != null);
            int length = path.length();
            checkArgument(length > 0);
            int firstSlash = path.indexOf(SLASH);
            checkArgument(firstSlash >= 0);
            if (length > 1) {
                boolean first = true;
                for (String component : SPLITTER.split(path)) {
                    // the only empty component should be the first one
                    if (first) {
                        first = false;
                        if (firstSlash == 0) {
                            assert (component.length() == 0);
                            continue;
                        }
                    }
                    Component.validate(component);
                }
            }
            return path;
        }

        // validates and strips out consecutive and trailing slashes
        public static String canonicalize(String path) {
            checkArgument(path != null);
            int length = path.length();
            checkArgument(length > 0);
            int firstSlash = path.indexOf(SLASH);
            checkArgument(firstSlash >= 0);
            String canonicalized = path;
            if (length > 1) {
                StringBuilder builder = new StringBuilder(length);
                if (firstSlash == 0) {
                    builder.append(SLASH);
                }
                canonicalized = 
                        JOINER.appendTo(builder, 
                                Iterables.transform(
                                        SPLITTER.omitEmptyStrings().split(path),
                                        Component.StringToString.VALIDATE))
                        .toString();
            }
            return canonicalized;
        }

        public static Path root() {
            return Reserved.ROOT.get();
        }

        @Serializer(input=String.class, output=ZNodeLabel.Path.class)
        public static Path of(String path) {
            return fromString(StringToString.VALIDATE, path);
        }
        
        public static Path canonicalized(String path) {
            return fromString(StringToString.CANONICALIZE, path);
        }

        protected static Path fromString(Function<String, String> transformer, String path) {
            String transformed = transformer.apply(path);
            if ((transformed.length() == 1) && (SLASH == transformed.charAt(0))) {
                return root();
            } else {
                return new Path(transformed);
            }
        }

        public static Path of(ZNodeLabel...components) {
            if (components.length == 0) {
                return root();
            } else {
                return of(Iterators.forArray(components));
            }
        }

        public static Path of(Iterator<? extends ZNodeLabel> components) {
            return joined(Iterators.transform(components, Functions.toStringFunction()));
        }
        
        public static Path joined(String...components) {
            if (components.length == 0) {
                return root();
            } else {
                return joined(Iterators.forArray(components));
            }
        }

        public static Path joined(Iterator<String> components) {
            String joined = join(components);
            if (joined.length() == 0) {
                return root();
            } else {
                int firstSlash = joined.indexOf(SLASH);
                checkArgument(firstSlash >= 0, joined);
                if (firstSlash == 0 && joined.length() == 1) {
                    return root();
                } else {
                    return new Path(joined);
                }
            }
        }

        public static String join(String...components) {
            return join(Iterators.forArray(components));
        }

        // validates but may return an empty String or a Component
        public static String join(Iterator<String> components) {
            StringBuilder builder = new StringBuilder();
            while (components.hasNext()) {
                String component = components.next();
                if (component == null) {
                    continue;
                }
                int firstSlash = component.indexOf(SLASH);
                if (firstSlash >= 0) {
                    Path.validate(component);
                } else {
                    Component.validate(component);
                }
                if (builder.length() == 0) {
                    builder.append(component);
                } else {
                    if (Path.root().toString().equals(component)) {
                        continue;
                    }
                    if ((firstSlash != 0) 
                            && (SLASH != builder.charAt(builder.length() - 1))) {
                        builder.append(SLASH);
                    }
                    builder.append(component);
                }
            }
            return builder.toString();
        }

        public static enum Reserved implements Singleton<Path> {
            ROOT(new Path(Character.toString(SLASH))), 
            ZOOKEEPER(Path.of(ROOT.get(), Component.Reserved.ZOOKEEPER.get())),
            QUOTA(Path.of(ZOOKEEPER.get(), Component.of("quota"))),
            CONFIG(Path.of(ZOOKEEPER.get(), Component.of("config")));
        
            private final Path value;
            
            private Reserved(Path value) {
                this.value = value;
            }
            
            @Override
            public Path get() {
                return value;
            }
        }

        public static enum StringToString implements Function<String, String> {
            VALIDATE {
                @Override
                public String apply(String input) {
                    return validate(input);
                }
            }, 
            CANONICALIZE {
                @Override
                public String apply(String input) {
                    return canonicalize(input);
                }
            };
        }

        protected Path(String label) {
            super(label);
        }

        public boolean isAbsolute() {
            return (SLASH == charAt(0));
        }

        public boolean isRoot() {
            return isAbsolute() && (length() == 1);
        }

        public ZNodeLabel head() {
            // TODO: memoize since we are immutable
            if (isRoot()) {
                return null;
            }
            int lastSlash = toString().lastIndexOf(SLASH);
            // we are canonicalized
            assert (lastSlash > 0) && (lastSlash < length() - 2) : toString();
            String head = toString().substring(0, lastSlash);
            if (head.indexOf(SLASH) >= 0) {
                return new Path(head);
            } else {
                return new Component(head);
            }
        }

        public Component tail() {
            // TODO: memoize since we are immutable
            if (isRoot()) {
                return null;
            }
            int lastSlash = toString().lastIndexOf(SLASH);
            // we are canonicalized
            assert (lastSlash > 0) && (lastSlash < length() - 2) : toString();
            String tail = toString().substring(lastSlash + 1);
            return new Component(tail);
        }

        public ZNodeLabel suffix() {
            String suffix = toString();
            int slash = suffix.indexOf(SLASH);
            suffix = suffix.substring(slash + 1);
            slash = suffix.indexOf(SLASH);
            if (slash > 0) {
                return new Path(suffix);
            } else {
                return new Component(suffix);
            }
        }
        
        public boolean prefixOf(ZNodeLabel.Path other) {
            String myPath = toString();
            String otherPath = other.toString();
            if (otherPath.startsWith(myPath)) {
                int length = myPath.length();
                if ((length == otherPath.length()) || SLASH == otherPath.charAt(length)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Iterator<Component> iterator() {
            // TODO: memoize
            return Iterables.transform(
                    SPLITTER.omitEmptyStrings().split(toString()),
                    Component.StringToComponent.OF).iterator();
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
    @Serializer(input=ZNodeLabel.class, output=String.class)
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
