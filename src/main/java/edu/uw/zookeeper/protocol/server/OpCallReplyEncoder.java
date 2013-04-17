package edu.uw.zookeeper.protocol.server;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.protocol.Encodable;
import edu.uw.zookeeper.protocol.Encoder;
import edu.uw.zookeeper.protocol.Records;

public class OpCallReplyEncoder implements Encoder<Operation.CallReply> {

    public static OpCallReplyEncoder create() {
        return new OpCallReplyEncoder();
    }

    public OpCallReplyEncoder() {
    }

    @Override
    public OutputStream encode(Operation.CallReply callReply,
            OutputStream stream) throws IOException {
        KeeperException.Code err = KeeperException.Code.OK;
        // unravel the layers...
        boolean unwrapping = true;
        Operation.Response response = callReply;
        while (unwrapping) {
            unwrapping = false;
            if (response instanceof Operation.Error) {
                err = ((Operation.Error) response).error();
            }
            if (response instanceof Operation.ResponseValue) {
                response = ((Operation.ResponseValue) response).response();
                unwrapping = true;
            }
        }
        Records.Responses.Headers.serialize(callReply.xid(), callReply.zxid(),
                err, stream);
        if (response instanceof Encodable) {
            stream = ((Encodable) response).encode(stream);
        }
        return stream;
    }
}