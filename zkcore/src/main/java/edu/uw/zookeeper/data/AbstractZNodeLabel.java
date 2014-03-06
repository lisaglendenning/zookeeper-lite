package edu.uw.zookeeper.data;

public abstract class AbstractZNodeLabel extends ZNodeName {
    
    public static RootZNodeLabel root() {
        return RootZNodeLabel.getInstance();
    }

    @Serializes(from=String.class, to=AbstractZNodeLabel.class)
    public static AbstractZNodeLabel fromString(String label) {
        if (label.isEmpty()) {
            return RootZNodeLabel.getInstance();
        } else {
            return ZNodeLabel.fromString(label);
        }
    }

    protected AbstractZNodeLabel(String label) {
        super(label);
    }

    public abstract boolean isRoot();
}