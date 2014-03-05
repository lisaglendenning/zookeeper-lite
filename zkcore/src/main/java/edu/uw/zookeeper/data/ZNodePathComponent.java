package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;

import edu.uw.zookeeper.common.Reference;

public final class ZNodePathComponent extends ZNodeLabel {

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

    // illegal unicode values
    private static final CharMatcher ILLEGAL_CHARACTERS = 
            CharMatcher.inRange('\u0000', '\u001f')
                .and(CharMatcher.inRange('\u007f', '\u009F'))
                .and(CharMatcher.inRange('\ud800', '\uf8ff'))
                .and(CharMatcher.inRange('\ufff0', '\uffff'))
                .precomputed();
    
    @Serializes(from=String.class, to=ZNodePathComponent.class)
    public static ZNodePathComponent of(String label) {
        return new ZNodePathComponent(label);
    }
    
    public static ZNodePathComponent validated(String label) {
        return new ZNodePathComponent(validate(label));
    }
    
    public static ZNodePathComponent zookeeper() {
        return Reserved.ZOOKEEPER.get();
    }

    public static ZNodePathComponent self() {
        return Reserved.SELF.get();
    }

    public static ZNodePathComponent parent() {
        return Reserved.PARENT.get();
    }

    private ZNodePathComponent(String label) {
        super(label);
    }
    
    public boolean isReserved() {
        return Reserved.contains(this);
    }
    
    public static enum Reserved implements Reference<ZNodePathComponent> {
        ZOOKEEPER(ZNodePathComponent.of("zookeeper")), 
        SELF(ZNodePathComponent.of(".")),
        PARENT(ZNodePathComponent.of(".."));
        
        public static boolean contains(ZNodePathComponent c) {
            for (ZNodePathComponent.Reserved e: values()) {
                if (e.get().equals(c)) {
                    return true;
                }
            }
            return false;
        }
        
        private final ZNodePathComponent value;
        
        private Reserved(ZNodePathComponent value) {
            this.value = value;
        }
        
        @Override
        public ZNodePathComponent get() {
            return value;
        }
    }

    public static enum StringToComponent implements Function<String, ZNodePathComponent> {
        OF;
    
        @Override
        public ZNodePathComponent apply(String input) {
            return ZNodePathComponent.of(input);
        }
    }
}