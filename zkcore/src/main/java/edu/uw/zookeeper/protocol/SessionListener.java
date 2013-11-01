package edu.uw.zookeeper.protocol;

import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;

public interface SessionListener extends Automatons.AutomatonListener<ProtocolState>, NotificationListener<Operation.ProtocolResponse<IWatcherEvent>> {
}