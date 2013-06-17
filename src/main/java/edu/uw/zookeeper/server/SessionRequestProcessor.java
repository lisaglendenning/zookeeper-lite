package edu.uw.zookeeper.server;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionReplyMessage;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;

public class SessionRequestProcessor extends Pair<Processor<Operation.Request, Operation.Response>, Processor<Operation.Response, Long>> implements Processor<Operation.SessionRequest, Operation.SessionReply> {

    public static SessionRequestProcessor newInstance(
            Processor<Operation.Request, Operation.Response> first,
            Processor<Operation.Response, Long> second) {
        return new SessionRequestProcessor(first, second);
    }
    
    public SessionRequestProcessor(
            Processor<Operation.Request, Operation.Response> first,
            Processor<Operation.Response, Long> second) {
        super(first, second);
    }

    @Override
    public SessionReplyMessage apply(Operation.SessionRequest input) throws Exception {
        Operation.Response reply = first().apply(input.request());
        Long zxid = second().apply(reply);
        
        int xid;
        if (reply instanceof Operation.XidHeader) {
            xid = ((Operation.XidHeader)reply).xid();
        } else {
            xid = input.xid();
        }

        return SessionReplyMessage.newInstance(xid, zxid, reply);
    }
}
