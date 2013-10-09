package edu.uw.zookeeper.net;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.google.common.base.Supplier;

public enum LoggingMarker implements Supplier<Marker> {
    NET_MARKER(MarkerManager.getMarker("EDU_UW_ZOOKEEPER_NET"));
    
    private final Marker marker;
    
    private LoggingMarker(Marker marker) {
        this.marker = marker;
    }
    
    @Override
    public Marker get() {
        return marker;
    }
}
