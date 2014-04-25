package edu.uw.zookeeper.data;

public abstract class AbstractWatchMatchListener implements WatchMatchListener {
    
    private final WatchMatcher matcher;
    
    protected AbstractWatchMatchListener(WatchMatcher matcher) {
        this.matcher = matcher;
    }
    
    @Override
    public final WatchMatcher getWatchMatcher() {
        return matcher;
    }
}
