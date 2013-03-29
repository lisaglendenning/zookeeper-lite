package org.apache.zookeeper.protocol.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Queue;
import javax.annotation.Nullable;

import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.Xid;
import org.apache.zookeeper.protocol.Decoder;
import org.apache.zookeeper.protocol.OpCallResult;
import org.apache.zookeeper.protocol.OpResult;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Pipeline;
import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.inject.Inject;

public class SessionStateDecoder implements Pipeline.Processor<Operation.Request>, Decoder<Operation.Response> {

    public static SessionStateDecoder create(Eventful eventful) {
        return new SessionStateDecoder(eventful);
    }

    public static SessionStateDecoder create(Eventful eventful,
            Xid xid) {
        return new SessionStateDecoder(eventful, xid);
    }

    public static class NextCallRequest implements Function<Integer, Operation> {
        protected Queue<Operation.Request> requests;
        
        public NextCallRequest(Queue<Operation.Request> requests) {
            this.requests = requests;
        }
         
        @Override
        @Nullable
        public Operation apply(@Nullable Integer xid) {
            Operation.Request nextRequest = requests.peek();
            if (nextRequest == null) {
                throw new IllegalArgumentException(
                        "No outstanding requests.");
            }
            
            if (! (nextRequest instanceof Operation.CallRequest)) {
                throw new IllegalArgumentException(
                        "Unexpected call response.");
            }
            
            int nextXid = ((Operation.CallRequest)nextRequest).xid();
            if (nextXid != xid) {
                throw new IllegalArgumentException(
                        String.format("Unexpected xid: %d != %d",
                                nextXid, xid));
            }
            
            return nextRequest.operation();
        }
    }

    protected final Logger logger = LoggerFactory.getLogger(SessionStateDecoder.class);
    protected final Queue<Operation.Request> requests;
    protected final SessionConnectionState state;
    protected final Pipeline<Operation.Request> processors;
    protected final SessionStateResponseDecoder decoder;
    protected final Function<Integer, Operation> xidToOp;

    protected SessionStateDecoder(Eventful eventful) {
        this(eventful, Xid.create());
    }
    
    @Inject
    protected SessionStateDecoder(Eventful eventful, Xid xid) {
        this(SessionConnectionState.create(eventful), xid);
    }
    
    protected SessionStateDecoder(SessionConnectionState state, Xid xid) {
        this(state, xid, new LinkedList<Operation.Request>());
    }

    protected SessionStateDecoder(
            SessionConnectionState state,
            Xid xid,
            Queue<Operation.Request> requests) {
        super();
        this.requests = requests;
        this.state = state;
        this.processors = new Pipeline<Operation.Request>();
        this.decoder = SessionStateResponseDecoder.create(state);
        this.xidToOp = new NextCallRequest(requests);

        processors.add(AssignXidProcessor.create(xid));
        processors.add(SessionStateRequestProcessor.create(state));
    }
    
    public SessionConnectionState state() {
        return state;
    }

    public Queue<Operation.Request> requests() {
        return requests;
    }
    
    @Override
    public Operation.Request apply(Operation.Request request) {
        request = processors.apply(request);
        // FIXME: I know we don't track pings or auth, TODO the other
        // special xid requests
        switch (request.operation()) {
        case AUTH:
        case PING:
            break;
        default:
            requests().add(request);
            break;
        }
        return request;
    }

    @Override
    public Operation.Response decode(InputStream stream) throws IOException {
        Operation.Response response = decoder.decode(xidToOp, stream);
        switch (response.operation()) {
        case AUTH:
        case PING:
            return response;
        default:
            break;
        }
        
        Queue<Operation.Request> requests = requests();
        Operation.Request request = requests.peek();
        if (request == null || request.operation() != response.operation()) { 
            throw new IllegalArgumentException(response.toString());
        }
        request = requests.poll();
        
        if (response instanceof Operation.CallResponse) {
            Operation.CallResponse callResponse = (Operation.CallResponse) response;
            Operation.CallRequest callRequest = null;
            if (request instanceof Operation.CallRequest) {
                callRequest = (Operation.CallRequest)request;
                
                // unwrap response
                if (response instanceof Operation.CallResult) {
                    callResponse = (Operation.CallResponse) ((Operation.CallResult) response).response();
                }
            } else if (response instanceof Operation.CallRequest) {
                callRequest = (Operation.CallRequest)response;
            } else {
                throw new IllegalArgumentException();
            }
            response = OpCallResult.create(callRequest, callResponse);
        } else {
            response = OpResult.create(request, response);
        }
        return response;
    }
}
