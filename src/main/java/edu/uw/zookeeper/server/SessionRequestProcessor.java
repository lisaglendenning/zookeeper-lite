package edu.uw.zookeeper.server;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionResponseMessage;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;

public class SessionRequestProcessor extends Pair<Processor<Records.Request, Records.Response>, Processor<Records.Response, Long>> implements Processor<Message.ClientRequest, Message.ServerResponse> {

    public static SessionRequestProcessor newInstance(
            Processor<Records.Request, Records.Response> first,
            Processor<Records.Response, Long> second) {
        return new SessionRequestProcessor(first, second);
    }
    
    public SessionRequestProcessor(
            Processor<Records.Request, Records.Response> first,
            Processor<Records.Response, Long> second) {
        super(first, second);
    }

    @Override
    public SessionResponseMessage apply(Message.ClientRequest input) throws Exception {
        Records.Response response = first().apply(input.request());
        Long zxid = second().apply(response);
        
        int xid;
        if (response instanceof Operation.RequestId) {
            xid = ((Operation.RequestId) response).xid();
        } else {
            xid = input.xid();
        }

        return SessionResponseMessage.newInstance(xid, zxid, response);
    }
}
