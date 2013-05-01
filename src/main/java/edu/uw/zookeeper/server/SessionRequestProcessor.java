package edu.uw.zookeeper.server;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionReplyWrapper;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;

public class SessionRequestProcessor extends Pair<Processor<? super Operation.Request, ? extends Operation.Reply>, Processor<? super Operation.Reply, ? extends Long>> implements Processor<Operation.SessionRequest, Operation.SessionReply> {

    public static SessionRequestProcessor newInstance(
            Processor<? super Operation.Request, ? extends Operation.Reply> first,
            Processor<? super Operation.Reply, ? extends Long> second) {
        return new SessionRequestProcessor(first, second);
    }
    
    public SessionRequestProcessor(
            Processor<? super Operation.Request, ? extends Operation.Reply> first,
            Processor<? super Operation.Reply, ? extends Long> second) {
        super(first, second);
    }

    @Override
    public Operation.SessionReply apply(Operation.SessionRequest input) throws Exception {
        Operation.Reply reply = first().apply(input.request());
        Long zxid = second().apply(reply);
        
        int xid;
        if (reply instanceof Operation.XidHeader) {
            xid = ((Operation.XidHeader)reply).xid();
        } else {
            xid = input.xid();
        }

        return SessionReplyWrapper.create(xid, zxid, reply);
    }
}
