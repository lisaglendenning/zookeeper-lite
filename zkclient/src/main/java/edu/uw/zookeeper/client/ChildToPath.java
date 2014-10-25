package edu.uw.zookeeper.client;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;

public final class ChildToPath implements Function<String, AbsoluteZNodePath> {

    public static ChildToPath forParent(ZNodePath parent) {
        return new ChildToPath(parent);
    }
    
    private final ZNodePath parent;
    
    protected ChildToPath(ZNodePath parent) {
        this.parent = parent;
    }
    
    @Override
    public AbsoluteZNodePath apply(String input) {
        return (AbsoluteZNodePath) parent.join(ZNodeLabel.fromString(input));
    }
}
