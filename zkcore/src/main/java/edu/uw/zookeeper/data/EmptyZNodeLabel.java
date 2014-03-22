package edu.uw.zookeeper.data;

import edu.uw.zookeeper.common.Reference;

public final class EmptyZNodeLabel extends AbstractZNodeLabel {

    public static EmptyZNodeLabel getInstance() {
        return Reserved.EMPTY.get();
    }
    
    private EmptyZNodeLabel() {
        super("");
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    protected static enum Reserved implements Reference<EmptyZNodeLabel> {
        EMPTY(new EmptyZNodeLabel());

        private final EmptyZNodeLabel instance;
        
        private Reserved(EmptyZNodeLabel instance) {
            this.instance = instance;
        }
        
        @Override
        public EmptyZNodeLabel get() {
            return instance;
        }
    }
}