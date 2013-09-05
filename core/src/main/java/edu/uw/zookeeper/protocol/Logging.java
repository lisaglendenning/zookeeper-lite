package edu.uw.zookeeper.protocol;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

public abstract class Logging {
    public static final Marker PING_MARKER = MarkerManager.getMarker("EDU_UW_ZOOKEEPER_PROTOCOL_PING");
}
