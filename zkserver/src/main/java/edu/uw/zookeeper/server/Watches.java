package edu.uw.zookeeper.server;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.protocol.NotificationListener;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;

/**
 * Not thread safe.
 */
public final class Watches {

    public static Watches create(
            Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners) {
        return create(listeners, HashMultimap.<String, Long>create(), HashMultimap.<Long, String>create());
    }
    
    public static Watches create(
            Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners,
            SetMultimap<String, Long> byPath,
            SetMultimap<Long, String> bySession) {
        return new Watches(listeners, byPath, bySession);
    }

    private final SetMultimap<String, Long> byPath;
    private final SetMultimap<Long, String> bySession;
    private final Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners;
    private final Logger logger;
    
    protected Watches(
            Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners,
            SetMultimap<String, Long> byPath,
            SetMultimap<Long, String> bySession) {
        this.logger = LogManager.getLogger(this);
        this.byPath = byPath;
        this.bySession = bySession;
        this.listeners = listeners;
    }
    
    public SetMultimap<String,Long> byPath() {
        return byPath;
    }
    
    public SetMultimap<Long, String> bySession() {
        return bySession;
    }
    
    public void post(final IWatcherEvent event) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}", WatchEvent.fromRecord(event));
        }
        final ProtocolResponseMessage<IWatcherEvent> message = ProtocolResponseMessage.of(
                OpCodeXid.NOTIFICATION.xid(), 
                OpCodeXid.NOTIFICATION_ZXID,
                event);
        String path = event.getPath();
        for (Long session: byPath.removeAll(path)) {
            bySession.remove(session, path);
            NotificationListener<Operation.ProtocolResponse<IWatcherEvent>> listener = listeners.apply(session);
            if (listener != null) {
                listener.handleNotification(message);
            }
        }
    }
    
    public void put(Long session, String path) {
        byPath.put(path, session);
        bySession.put(session, path);
    }
    
    public Set<String> remove(Long session) {
        Set<String> paths = bySession.removeAll(session);
        for (String path: paths) {
            byPath.remove(path, session);
        }
        return paths;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).toString();
    }
}