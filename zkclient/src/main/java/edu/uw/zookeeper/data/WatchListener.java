package edu.uw.zookeeper.data;

import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.protocol.ProtocolState;

public interface WatchListener extends Automatons.AutomatonListener<ProtocolState> {
    void handleWatchEvent(WatchEvent event);
}