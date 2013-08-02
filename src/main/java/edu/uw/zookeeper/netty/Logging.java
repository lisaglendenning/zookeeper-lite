package edu.uw.zookeeper.netty;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

public abstract class Logging extends edu.uw.zookeeper.net.Logging {

    public static final Marker NETTY_MARKER = MarkerManager.getMarker("edu.uw.zookeeeper.netty", NET_MARKER);
}
