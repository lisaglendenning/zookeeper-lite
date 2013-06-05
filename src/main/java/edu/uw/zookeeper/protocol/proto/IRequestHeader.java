package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.RequestHeader;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records.HeaderRecord;
import edu.uw.zookeeper.protocol.Records.Headers;
import edu.uw.zookeeper.protocol.Records.OperationRecord;
import edu.uw.zookeeper.protocol.Records.TaggedRecord;

public class IRequestHeader extends RequestHeader implements HeaderRecord, TaggedRecord, OperationRecord {
    
    public IRequestHeader() {
        super();
    }

    public IRequestHeader(int xid, int type) {
        super(xid, type);
    }

    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Headers.TAG); 
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Headers.TAG);
    }

    @Override
    public OpCode opcode() {
        return OpCode.of(getType());
    }
}