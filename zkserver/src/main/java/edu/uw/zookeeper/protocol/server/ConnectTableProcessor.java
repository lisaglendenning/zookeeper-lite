package edu.uw.zookeeper.protocol.server;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.server.SessionTable;

public class ConnectTableProcessor 
        implements Processor<ConnectMessage.Request, ConnectMessage.Response> {

    public static ConnectTableProcessor create(
            SessionTable sessions,
            ZxidReference lastZxid) {
        return new ConnectTableProcessor(sessions, lastZxid, false);
    }
    
    protected final Logger logger = LogManager
            .getLogger(ConnectTableProcessor.class);
    protected final SessionTable sessions;
    protected final ZxidReference lastZxid;
    protected final boolean readOnly;

    protected ConnectTableProcessor(
            SessionTable sessions,
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
    
    public SessionTable sessions() {
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
