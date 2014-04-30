package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.CharMatcher;

import edu.uw.zookeeper.common.Reference;

public final class ZNodeLabel extends AbstractZNodeLabel {
    
    /**
     * @param label to be validated
     * @return label
     */
    public static <T extends CharSequence> T validate(T label) {
        return validate(label, 0, label.length());
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends CharSequence> T validate(T label, int start, int end) {
        checkArgument(start < end, "empty label");
        for (int i=start; i<end; ++i) {
            char c = label.charAt(i);
            if (c == SLASH) {
                throw new IllegalArgumentException(String.format("Slash at index %d of %s", i, label));
            } else if (ILLEGAL_CHARACTERS.matches(c)) {
                throw new IllegalArgumentException(String.format("Illegal character at index %d of %s", i, label));
            }
        }
        return ((start > 0) || (end < label.length() - 1)) ? (T) label.subSequence(start, end) : label;
    }

    public static ZNodeLabel validated(String label) {
        return fromString(validate(label));
    }

    public static ZNodeLabel validated(String label, int start, int end) {
        return fromString(validate(label, start, end));
    }
    
    @Serializes(from=String.class, to=ZNodeLabel.class)
    public static ZNodeLabel fromString(String label) {
        assert(! label.isEmpty());
        return new ZNodeLabel(label);
    }
    
    public static ZNodeLabel zookeeper() {
        return Reserved.ZOOKEEPER.get();
    }

    public static ZNodeLabel self() {
        return Reserved.SELF.get();
    }

    public static ZNodeLabel parent() {
        return Reserved.PARENT.get();
    }

    private ZNodeLabel(String label) {
        super(label);
    }
    
    @Override
    public boolean isEmpty() {
        return false;
    }
    
    public boolean isReserved() {
        return Reserved.contains(this);
    }
    
    // illegal unicode values
    private static final CharMatcher ILLEGAL_CHARACTERS = 
            CharMatcher.inRange('\u0000', '\u001f')
                .and(CharMatcher.inRange('\u007f', '\u009F'))
                .and(CharMatcher.inRange('\ud800', '\uf8ff'))
                .and(CharMatcher.inRange('\ufff0', '\uffff'))
                .precomputed();

    public static enum Reserved implements Reference<ZNodeLabel> {
        ZOOKEEPER(ZNodeLabel.fromString("zookeeper")), 
        SELF(ZNodeLabel.fromString(".")),
        PARENT(ZNodeLabel.fromString(".."));
        
        public static boolean contains(ZNodeLabel c) {
            for (ZNodeLabel.Reserved e: values()) {
                if (e.get().equals(c)) {
                    return true;
                }
            }
            return false;
        }
        
        private final ZNodeLabel value;
        
        private Reserved(ZNodeLabel value) {
            this.value = value;
        }
        
        @Override
        public ZNodeLabel get() {
            return value;
        }
    }
}