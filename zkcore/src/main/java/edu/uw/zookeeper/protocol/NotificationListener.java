package edu.uw.zookeeper.protocol;

import edu.uw.zookeeper.protocol.proto.IWatcherEvent;

public interface NotificationListener<T extends Operation.ProtocolResponse<IWatcherEvent>> {
    void handleNotification(T notification);
}
