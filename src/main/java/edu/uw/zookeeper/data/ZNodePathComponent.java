package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

//TODO: check if unicode is handled correctly
public class ZNodePathComponent implements CharSequence {
    public static final Set<String> ILLEGAL = ImmutableSet.of(".", "..");
    
    public static final ZNodePathComponent ZOOKEEPER = new ZNodePathComponent("zookeeper");

    public static ZNodePathComponent of(String name) {
        String validated = validate(name);
        return new ZNodePathComponent(validated);
    }
    
    public static String validate(String name) {
        checkArgument(name != null);
        checkArgument(name.length() > 0);
        checkArgument(! ILLEGAL.contains(name));
        checkArgument(name.indexOf(ZNodePath.SLASH) == -1);
        // TODO: check for illegal unicode values:
        // u0000, u0001 - u0019, u007F - u009F,
        // ud800 - uF8FFF, uFFF0 - uFFFF,
        // uXFFFE - uXFFFF (where X is a digit 1 - E), 
        // uF0000 - uFFFFF.
        return name;
    }

    private final String name;

    private ZNodePathComponent(String name) {
        this.name = name;
    }
    
    public boolean isReserved() {
        return equals(ZOOKEEPER);
    }
    
    @Override
    public String toString() {
        return name;
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
    public int length() {
        return toString().length();
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
        ZNodePathComponent other = (ZNodePathComponent) obj;
        return (Objects.equal(toString(), other.toString()));
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}