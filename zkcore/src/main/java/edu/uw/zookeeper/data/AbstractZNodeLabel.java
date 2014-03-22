package edu.uw.zookeeper.data;

public abstract class AbstractZNodeLabel extends ZNodeName {
    
    public static EmptyZNodeLabel empty() {
        return EmptyZNodeLabel.getInstance();
    }

    @Serializes(from=String.class, to=AbstractZNodeLabel.class)
    public static AbstractZNodeLabel fromString(String label) {
        if (label.isEmpty()) {
            return EmptyZNodeLabel.getInstance();
        } else {
            return ZNodeLabel.fromString(label);
        }
    }

    protected AbstractZNodeLabel(String label) {
        super(label);
    }

    public abstract boolean isEmpty();
}