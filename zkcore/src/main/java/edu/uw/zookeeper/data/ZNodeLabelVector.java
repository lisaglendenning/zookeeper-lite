package edu.uw.zookeeper.data;

import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;

public abstract class ZNodeLabelVector extends ZNodeName implements Iterable<ZNodeLabel> {

    public static String join(String...labels) {
        return join(Iterators.forArray(labels));
    }

    public static String join(Iterator<String> labels) {
        StringBuilder builder = new StringBuilder();
        while (labels.hasNext()) {
            String label = labels.next();
            if (label == null) {
                continue;
            }
            if (builder.length() == 0) {
                builder.append(label);
            } else if (! label.isEmpty()) {
                if (label.charAt(0) == SLASH) {
                    if (label.length() == 1) {
                        continue;
                    }
                    if (SLASH == builder.charAt(builder.length() - 1)) {
                        builder.append(label, 1, label.length());
                        continue;
                    }
                } else {
                    if (SLASH != builder.charAt(builder.length() - 1)) {
                        builder.append(SLASH);
                    }
                }
                builder.append(label);
            }
        }
        return builder.toString();
    }

    @Serializes(from=String.class, to=ZNodeLabelVector.class)
    public static ZNodeLabelVector fromString(String path) {
        if (path.charAt(0) == SLASH) {
            return ZNodePath.fromString(path);
        } else {
            return RelativeZNodePath.fromString(path);
        }
    }

    public static String headOf(String path) {
        int lastSlash = path.lastIndexOf(ZNodeName.SLASH);
        if (lastSlash == -1) {
            return "";
        } else if (lastSlash == 0) {
            return (path.length() > 1) ? "/" : "";
        } else {
            return path.substring(0, lastSlash);
        }
    }

    public static String tailOf(String path) {
        int lastSlash = path.lastIndexOf(ZNodeName.SLASH);
        if ((lastSlash == -1) || (lastSlash == path.length() - 1)) {
            return "";
        } else {
            return path.substring(lastSlash + 1);
        }
    }

    public static enum StringToLabel implements Function<String, ZNodeLabel> {
        FROM_STRING;
    
        @Override
        public ZNodeLabel apply(String input) {
            return ZNodeLabel.fromString(input);
        }
    }
    
    private static final Splitter SPLITTER = Splitter.on(SLASH);

    protected ZNodeLabelVector(String label) {
        super(label);
    }

    public abstract boolean isAbsolute();
    
    public abstract ZNodeLabelVector join(ZNodeName other);

    @Override
    public boolean startsWith(ZNodeName prefix) {
        int prefixLength = prefix.length();
        return toString().startsWith(prefix.toString()) && ((length() == prefixLength) || (charAt(prefixLength) == SLASH));
    }

    public ZNodeName prefix(int index) {
        int length = length();
        if ((index < 0) || (index > length)) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        if (index == 0) {
            return RootZNodeLabel.getInstance();
        } else if (index == length) {
            return this;
        } else {
            if (charAt(index) != SLASH) {
                throw new IllegalArgumentException(String.format("Cannot split prefix at index %d for %s", index, this));
            }
            return ZNodeName.fromString(toString().substring(0, index));
        }
    }

    public ZNodeName suffix(int index) {
        int length = length();
        if ((index < 0) || (index > length)) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        if (index == length) {
            return RootZNodeLabel.getInstance();
        } else {
            if (charAt(index) != SLASH) {
                throw new IllegalArgumentException(String.format("Cannot split suffix at index %d for %s", index, this));
            }
            return ZNodeName.fromString(toString().substring(index + 1));
        }
    }

    @Override
    public Iterator<ZNodeLabel> iterator() {
        // TODO: memoize?
        return Iterators.transform(
                SPLITTER.omitEmptyStrings().split(toString()).iterator(),
                StringToLabel.FROM_STRING);
    }
}