package edu.uw.zookeeper.data;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

import edu.uw.zookeeper.common.Reference;

public final class RootZNodePath extends ZNodePath {
    
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
    public EmptyZNodeLabel label() {
        return EmptyZNodeLabel.getInstance();
    }

    @Override
    public ZNodePath join(ZNodeName other) {
        String suffix = other.toString();
        if (suffix.isEmpty()) {
            return this;
        } else if (suffix.charAt(0) == SLASH) {
            throw new IllegalArgumentException(suffix);
        }
        return AbsoluteZNodePath.fromString(new StringBuilder(suffix.length() + 1).append(SLASH).append(suffix).toString());
    }
    
    @Override
    public UnmodifiableIterator<ZNodeLabel> iterator() {
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