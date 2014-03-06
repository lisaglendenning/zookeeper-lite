package edu.uw.zookeeper.data;

import edu.uw.zookeeper.common.Reference;

public final class RootZNodeLabel extends AbstractZNodeLabel {

    public static RootZNodeLabel getInstance() {
        return Reserved.ROOT.get();
    }
    
    private RootZNodeLabel() {
        super("");
    }

    @Override
    public boolean isRoot() {
        return true;
    }

    protected static enum Reserved implements Reference<RootZNodeLabel> {
        ROOT(new RootZNodeLabel());

        private final RootZNodeLabel instance;
        
        private Reserved(RootZNodeLabel instance) {
            this.instance = instance;
        }
        
        @Override
        public RootZNodeLabel get() {
            return instance;
        }
    }
}