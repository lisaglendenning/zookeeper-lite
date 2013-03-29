package org.apache.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;

import com.google.common.base.Function;

public class Operations {
    
    public static class Requests {
        @SuppressWarnings("unchecked")
        public static <T extends Operation.Request> T create(Operation op) {
            Operation.Request request;
            switch(op) {
            case CREATE_SESSION:
                request = OpCreateSessionAction.Request.create();
                break;
            case AUTH:
                request = OpAuthRequest.create();
                break;
            case PING:
                request = OpPingAction.Request.create();
                break;
            case CLOSE_SESSION:
                request = OpAction.Request.create(op);
                break;
            default:
                request = OpRecordAction.Request.create(op);
            }
            return (T) request;
        }

        @SuppressWarnings("unchecked")
        public static <T extends Operation.Request> T decode(Operation op, InputStream stream) throws IOException {
            Operation.Request request = create(op);
            if (request instanceof Decodable) {
                request = (Operation.Request) ((Decodable)request).decode(stream);
            }
            return (T) request;
        }
        
        public static Operation.CallRequest decode(InputStream stream) throws IOException {
            RequestHeader header = Records.Requests.Headers.deserialize(stream);
            Operation op = Operation.get(header.getType());
            Operation.Request request = decode(op, stream);
            if (!(request instanceof Operation.CallRequest)) {
                request = OpCallRequest.create(header.getXid(), request);
            }
            return (Operation.CallRequest) request;
        }
    }
    
    public static class Responses {
        @SuppressWarnings("unchecked")
        public static <T extends Operation.Response> T create(Operation op) {
            Operation.Response response;
            switch(op) {
            case CREATE_SESSION:
                response = OpCreateSessionAction.Response.create();
                break;
            case NOTIFICATION:
                response = OpNotificationResponse.create();
                break;
            case PING:
                response = OpPingAction.Response.create();
                break;
            case AUTH:
            case CLOSE_SESSION:
                response = OpAction.Response.create(op);
                break;
            default:
                response = OpRecordAction.Response.create(op);
            }
            return (T) response;
        }

        @SuppressWarnings("unchecked")
        public static <T extends Operation.Response> T decode(Operation op, InputStream stream) throws IOException {
            Operation.Response response = create(op);
            if (response instanceof Decodable) {
                response = (Operation.Response) ((Decodable)response).decode(stream);
            }
            return (T) response;
        }
        
        @SuppressWarnings("rawtypes")
        public static Operation.CallResult decode(Function<Integer, Operation> xidToOp, InputStream stream) throws IOException {
            Operation.Response response = null;
            ReplyHeader header = Records.Responses.Headers.deserialize(stream);
            Operation op = null;
            int xid = header.getXid();
            if (Records.OperationXid.has(xid)) {
                op = Records.OperationXid.get(xid).operation();
            } else {
                op = xidToOp.apply(header.getXid());
            }
            checkArgument(op != null);
            
            KeeperException.Code err = KeeperException.Code.get(header.getErr());
            checkArgument(err != null);
            if (err != KeeperException.Code.OK) {
                // FIXME: I'm not sure if there's ever a record following an error header?
                // It doesn't look like the client expects one
                response = OpAction.Response.create(op);
                response = OpError.create(response, err);
            } else {           
                response = decode(op, stream);
            }

            long zxid = header.getZxid();
            Operation.CallResult result = OpCallResult.create(
                        OpCallRequest.create(xid, OpAction.Request.create(op)), 
                        OpCallResponse.create(zxid, response));
            return result;
        }
    }
}
