package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.AuthPacket;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records.RequestRecord;
import edu.uw.zookeeper.protocol.proto.Records.Requests;

public class IAuthRequest extends AuthPacket implements RequestRecord, Operation.XidHeader {
    public static final OpCodeXid OPCODE_XID = OpCodeXid.AUTH;
    public static final OpCode OPCODE = OPCODE_XID.opcode();
    public static final int XID = OPCODE_XID.xid();
           
    @Override
    public OpCode opcode() {
        return OPCODE;
    }

    @Override
    public int xid() {
        return XID;
    }
    
    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Requests.TAG);
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Requests.TAG);
    }
}