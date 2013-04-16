package edu.uw.zookeeper.data;

import java.io.IOException;
import java.io.InputStream;

import org.apache.zookeeper.proto.AuthPacket;

import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.Records.OperationXid;
import edu.uw.zookeeper.protocol.Records.Requests;

public class OpAuthRequest extends OpRecordAction.Request<AuthPacket> implements
        Operation.CallRequest {

    public static OpAuthRequest create() {
        return new OpAuthRequest();
    }

    public static OpAuthRequest create(AuthPacket record) {
        return new OpAuthRequest(record);
    }

    public static Records.OperationXid opXid() {
        return Records.OperationXid.AUTH;
    }

    public static AuthPacket createRecord() {
        return Records.Requests.<AuthPacket> create(opXid().operation());
    }

    public OpAuthRequest() {
        this(createRecord());
    }

    public OpAuthRequest(AuthPacket record) {
        super(record);
    }

    @Override
    public Operation operation() {
        return opXid().operation();
    }

    @Override
    public int xid() {
        return opXid().xid();
    }

    @Override
    public OpRecordAction.Request<AuthPacket> decode(InputStream stream)
            throws IOException {
        if (record() == null) {
            setRecord(createRecord());
        }
        return super.decode(stream);
    }
}
