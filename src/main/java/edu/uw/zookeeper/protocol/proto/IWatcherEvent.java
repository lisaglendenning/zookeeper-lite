package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.WatcherEvent;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records.ResponseRecord;

public class IWatcherEvent extends WatcherEvent implements ResponseRecord, Records.PathRecord, Operation.XidHeader {
    public static final OpCodeXid OPCODE_XID = OpCodeXid.NOTIFICATION;
    public static final OpCode OPCODE = OPCODE_XID.opcode();
    public static final int XID = OPCODE_XID.xid();
    
    public IWatcherEvent() {
        super();
    }

    public IWatcherEvent(int type, int state, String path) {
        super(type, state, path);
    }

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
        serialize(archive, Records.NOTIFICATION_TAG);
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Records.NOTIFICATION_TAG);
    }
}