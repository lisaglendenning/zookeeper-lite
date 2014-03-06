package edu.uw.zookeeper.data;

import edu.uw.zookeeper.common.Reference;

public final class AbsoluteZNodePath extends ZNodePath {

    public static AbsoluteZNodePath zookeeper() {
        return AbsoluteZNodePath.Reserved.ZOOKEEPER.get();
    }

    @Serializes(from=String.class, to=AbsoluteZNodePath.class)
    public static AbsoluteZNodePath fromString(String path) {
        assert (path.charAt(0) == SLASH);
        assert (path.length() > 1);
        return new AbsoluteZNodePath(path);
    }
    
    private AbsoluteZNodePath(String label) {
        super(label);
    }

    @Override
    public boolean isRoot() {
        return false;
    }

    public ZNodePath parent() {
        // TODO: memoize?
        int lastSlash = toString().lastIndexOf(SLASH);
        String parent = toString().substring(0, lastSlash);
        return ZNodePath.fromString(parent);
    }

    @Override
    public ZNodeLabel label() {
        // TODO: memoize?
        int lastSlash = toString().lastIndexOf(SLASH);
        String tail = toString().substring(lastSlash + 1);
        return ZNodeLabel.fromString(tail);
    }

    @Override
    public ZNodePath join(ZNodeName other) {
        String suffix = other.toString();
        if (suffix.isEmpty()) {
            return this;
        } else if (suffix.charAt(0) == SLASH) {
            throw new IllegalArgumentException(suffix);
        }
        return AbsoluteZNodePath.fromString(new StringBuilder(length() + suffix.length() + 1).append(toString()).append(SLASH).append(suffix).toString());
    }
    
    public static enum Reserved implements Reference<AbsoluteZNodePath> {
        ZOOKEEPER((AbsoluteZNodePath) root().join(ZNodeLabel.zookeeper())),
        QUOTA((AbsoluteZNodePath) ZOOKEEPER.get().join(ZNodeLabel.fromString("quota"))),
        CONFIG((AbsoluteZNodePath)ZOOKEEPER.get().join(ZNodeLabel.fromString("config")));
    
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
