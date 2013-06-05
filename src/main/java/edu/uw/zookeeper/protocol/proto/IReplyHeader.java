package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.ReplyHeader;

import edu.uw.zookeeper.protocol.Records.HeaderRecord;
import edu.uw.zookeeper.protocol.Records.Headers;
import edu.uw.zookeeper.protocol.Records.TaggedRecord;

public class IReplyHeader extends ReplyHeader implements TaggedRecord, HeaderRecord {
    
    public IReplyHeader() {
        super();
    }

    public IReplyHeader(int xid, long zxid, int err) {
        super(xid, zxid, err);
    }

    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Headers.TAG); 
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Headers.TAG);
    }
}