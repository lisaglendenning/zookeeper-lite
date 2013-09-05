package edu.uw.zookeeper.netty;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

public abstract class Logging extends edu.uw.zookeeper.net.Logging {

    public static final Marker NETTY_MARKER = MarkerManager.getMarker("EDU_UW_ZOOKEEPER_PROTOCOL_NETTY", NET_MARKER);
}
