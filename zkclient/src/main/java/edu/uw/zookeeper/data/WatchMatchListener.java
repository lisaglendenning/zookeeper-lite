package edu.uw.zookeeper.data;

public interface WatchMatchListener extends WatchListener {
    WatchMatcher getWatchMatcher();
}