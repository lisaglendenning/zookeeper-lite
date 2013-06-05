package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;
import java.util.List;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.SetWatches;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Records.OpCodeXid;
import edu.uw.zookeeper.protocol.Records.RequestRecord;
import edu.uw.zookeeper.protocol.Records.Requests;

public class ISetWatchesRequest extends SetWatches implements RequestRecord, Operation.XidHeader {
    public static final OpCodeXid OPCODE_XID = OpCodeXid.SET_WATCHES;
    public static final OpCode OPCODE = OPCODE_XID.opcode();
    public static final int XID = OPCODE_XID.xid();
           
    public ISetWatchesRequest() {
        super();
    }

    public ISetWatchesRequest(long relativeZxid, List<String> dataWatches,
            List<String> existWatches, List<String> childWatches) {
        super(relativeZxid, dataWatches, existWatches, childWatches);
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
        serialize(archive, Requests.TAG);
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Requests.TAG);
    }
}