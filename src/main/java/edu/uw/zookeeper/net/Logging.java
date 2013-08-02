package edu.uw.zookeeper.net;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

public abstract class Logging {

    public static final Marker NET_MARKER = MarkerManager.getMarker(Logging.class.getPackage().getName());
}
