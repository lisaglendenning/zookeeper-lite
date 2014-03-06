package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.*;

public abstract class ZNodeName implements CharSequence, Comparable<ZNodeName> {

    public static final char SLASH = '/';
    
    @Serializes(from=String.class, to=ZNodeName.class)
    public static ZNodeName fromString(String label) {
        if (label.indexOf(SLASH) >= 0) {
            return ZNodeLabelVector.fromString(label);
        } else {
            return ZNodeLabel.fromString(label);
        }
    }

    private final String name;

    protected ZNodeName(String label) {
        this.name = checkNotNull(label);
    }
    
    public boolean startsWith(ZNodeName prefix) {
        return name.equals(prefix.name);
    }

    @Override
    public int length() {
        return name.length();
    }

    @Override
    public char charAt(int arg0) {
        return name.charAt(arg0);
    }

    @Override
    public CharSequence subSequence(int arg0, int arg1) {
        return name.subSequence(arg0, arg1);
    }

    @Override
    public int compareTo(ZNodeName other) {
        return name.compareTo(other.name);
    }

    @Serializes(from=ZNodeName.class, to=String.class)
    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof ZNodeName)) {
            return false;
        }
        ZNodeName other = (ZNodeName) obj;
        return name.equals(other.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
