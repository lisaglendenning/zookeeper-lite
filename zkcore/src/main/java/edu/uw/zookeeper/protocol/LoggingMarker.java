package edu.uw.zookeeper.protocol;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.google.common.base.Supplier;

public enum LoggingMarker implements Supplier<Marker> {
    PROTOCOL_MARKER(MarkerManager.getMarker("EDU_UW_ZOOKEEPER_PROTOCOL")),
    PING_MARKER(MarkerManager.getMarker("EDU_UW_ZOOKEEPER_PROTOCOL_PING").addParents( 
            PROTOCOL_MARKER.get()));

    private final Marker marker;
    
    private LoggingMarker(Marker marker) {
        this.marker = marker;
    }
    
    @Override
    public Marker get() {
        return marker;
    }
}
