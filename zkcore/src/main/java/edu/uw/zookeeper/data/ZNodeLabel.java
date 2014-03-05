package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.*;
import edu.uw.zookeeper.common.Reference;

public abstract class ZNodeLabel implements CharSequence, Comparable<ZNodeLabel> {

    public static final char SLASH = '/';
    
    @Serializes(from=String.class, to=ZNodeLabel.class)
    public static ZNodeLabel of(String label) {
        if (label.length() == 0) {
            return None.getInstance();
        } else if (label.indexOf(SLASH) >= 0) {
            return ZNodePath.of(label);
        } else {
            return ZNodePathComponent.of(label);
        }
    }
    
    public static None none() {
        return None.getInstance();
    }

    private final String label;

    protected ZNodeLabel(String label) {
        this.label = checkNotNull(label);
    }
    
    public boolean isNone() {
        return (label.length() > 0);
    }
    
    public boolean startsWith(ZNodeLabel prefix) {
        return label.equals(prefix.label);
    }

    @Override
    public int length() {
        return label.length();
    }

    @Override
    public char charAt(int arg0) {
        return label.charAt(arg0);
    }

    @Override
    public CharSequence subSequence(int arg0, int arg1) {
        return label.subSequence(arg0, arg1);
    }

    @Override
    public int compareTo(ZNodeLabel other) {
        return label.compareTo(other.label);
    }

    @Serializes(from=ZNodeLabel.class, to=String.class)
    @Override
    public String toString() {
        return label;
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
        return label.equals(other.label);
    }

    @Override
    public int hashCode() {
        return label.hashCode();
    }

    public static final class None extends ZNodeLabel {
    
        public static None getInstance() {
            return Reserved.NONE.get();
        }
        
        private None() {
            super("");
        }
    
        @Override
        public boolean isNone() {
            return true;
        }
    
        protected static enum Reserved implements Reference<None> {
            NONE(new None());
    
            private final None instance;
            
            private Reserved(None instance) {
                this.instance = instance;
            }
            
            @Override
            public None get() {
                return instance;
            }
        }
    }
}
