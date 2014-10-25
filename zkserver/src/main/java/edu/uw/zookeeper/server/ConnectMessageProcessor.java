package edu.uw.zookeeper.server;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.ZxidReference;

public class ConnectMessageProcessor 
        implements Function<ConnectMessage.Request, ConnectMessage.Response> {

    public static ConnectMessageProcessor defaults(
            SessionManager sessions,
            ZxidReference lastZxid) {
        return new ConnectMessageProcessor(
                ImmutableList.<Function<ConnectMessage.Request, KeeperException.Code>>of(
                        new ZxidValidator(lastZxid), 
                        new ReadOnlyValidator(false),
                        new ExistingSessionValidator(sessions)),
                sessions);
    }

    protected final Logger logger;
    protected final ImmutableList<Function<ConnectMessage.Request, KeeperException.Code>> validators;
    protected final SessionManager sessions;

    protected ConnectMessageProcessor(
            ImmutableList<Function<ConnectMessage.Request, KeeperException.Code>> validators,
            SessionManager sessions) {
        this.logger = LogManager.getLogger(this);
        this.validators = validators;
        this.sessions = sessions;
    }

    @Override
    public ConnectMessage.Response apply(ConnectMessage.Request input) {
        for (Function<ConnectMessage.Request, KeeperException.Code> validator: validators) {
            KeeperException.Code code = validator.apply(input);
            if (code != KeeperException.Code.OK) {
                throw new IllegalArgumentException(KeeperException.create(code));
            }
        }
        
        Session.Parameters parameters = input.toParameters();
        final Session session;
        if (input instanceof ConnectMessage.Request.NewRequest) {
            session = sessions.create(parameters.timeOut());
            logger.info("Created session {}", Session.toString(session.id()));
        } else {
            session = sessions.get(input.getSessionId());
            if (session == null) {
                throw new IllegalArgumentException(new KeeperException.UnknownSessionException());
            }
            logger.info("Renewed session {}", Session.toString(input.getSessionId()));
        }
        
        return ConnectMessage.Response.Valid.newInstance(session, input.getReadOnly(), input.legacy());
    }
    
    public static final class ZxidValidator implements Function<ConnectMessage.Request, KeeperException.Code> {

        private final ZxidReference lastZxid;

        public ZxidValidator(
                ZxidReference lastZxid) {
            this.lastZxid = lastZxid;
        }
        
        public ZxidReference lastZxid() {
            return lastZxid;
        }
        
        @Override
        public KeeperException.Code apply(ConnectMessage.Request input) {
            // note that the behavior of ZooKeeperServer 
            // is to just close the connection
            // without replying when the zxid is out of sync
            long zxid = lastZxid.get();
            if (input.getLastZxidSeen() > zxid) {
                return KeeperException.Code.BADARGUMENTS;
            }
            return KeeperException.Code.OK;
        }
    }
    
    public static final class ReadOnlyValidator implements Function<ConnectMessage.Request, KeeperException.Code> {

        private final boolean readOnly;

        public ReadOnlyValidator(
                boolean readOnly) {
            this.readOnly = readOnly;
        }
        
        public boolean readOnly() {
            return readOnly;
        }
        
        @Override
        public KeeperException.Code apply(ConnectMessage.Request input) {
            if (readOnly() && !input.getReadOnly()) {
                return KeeperException.Code.NOTREADONLY;
            }
            return KeeperException.Code.OK;
        }
    }
    
    public static final class ExistingSessionValidator implements Function<ConnectMessage.Request, KeeperException.Code> {

        private final SessionManager sessions;
        
        public ExistingSessionValidator(SessionManager sessions) {
            this.sessions = sessions;
        }
        
        public SessionManager sessions() {
            return sessions;
        }
        
        @Override
        public KeeperException.Code apply(ConnectMessage.Request input) {
            if (input instanceof ConnectMessage.Request.RenewRequest) {
                Session session = sessions.get(input.getSessionId());
                if (session != null) {
                    if (! Arrays.equals(session.parameters().password(), input.getPasswd())) {
                        return KeeperException.Code.BADARGUMENTS;
                    }
                } else {
                    return KeeperException.Code.UNKNOWNSESSION;
                }
            }
            return KeeperException.Code.OK;
        }
    }
}
