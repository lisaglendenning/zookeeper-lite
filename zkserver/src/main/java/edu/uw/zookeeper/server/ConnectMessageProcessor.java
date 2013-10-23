package edu.uw.zookeeper.server;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.ZxidReference;

public class ConnectMessageProcessor 
        implements Processor<ConnectMessage.Request, ConnectMessage.Response> {

    public static ConnectMessageProcessor create(
            SessionManager sessions,
            ZxidReference lastZxid) {
        return new ConnectMessageProcessor(sessions, lastZxid, false);
    }
    
    protected final Logger logger = LogManager
            .getLogger(ConnectMessageProcessor.class);
    protected final SessionManager sessions;
    protected final ZxidReference lastZxid;
    protected final boolean readOnly;

    protected ConnectMessageProcessor(
            SessionManager sessions,
            ZxidReference lastZxid,
            boolean readOnly) {
        this.sessions = sessions;
        this.lastZxid = lastZxid;
        this.readOnly = readOnly;
    }
    
    public boolean readOnly() {
        return readOnly;
    }

    public ZxidReference lastZxid() {
        return lastZxid;
    }
    
    public SessionManager sessions() {
        return sessions;
    }

    @Override
    public ConnectMessage.Response apply(ConnectMessage.Request request) {
        // emulating the the behavior of ZooKeeperServer,
        // which is to just close the connection
        // without replying when the zxid is out of sync
        long myZxid = lastZxid().get();
        if (request.getLastZxidSeen() > myZxid) {
            throw new IllegalStateException(String.format("Zxid 0x%x > 0x%x",
                    Long.toHexString(request.getLastZxidSeen()),
                    Long.toHexString(myZxid)));
        }
        
        if (readOnly() && ! request.getReadOnly()) {
            throw new IllegalStateException("readonly");
        }
        
        Session session = null;
        if (request instanceof ConnectMessage.Request.NewRequest) {
            session = sessions().validate(request.toParameters());
        } else if (request instanceof ConnectMessage.Request.RenewRequest) {
            try {
                session = sessions().validate(request.toSession());
            } catch (Exception e) {
                session = null;
            }
        } else {
            throw new IllegalArgumentException(request.toString());
        }
        
        ConnectMessage.Response response = (session == null)
            ? ConnectMessage.Response.Invalid.newInstance(request.getReadOnly(), request.legacy())
            : ConnectMessage.Response.Valid.newInstance(session, request.getReadOnly(), request.legacy());
        return response;
    }
}
