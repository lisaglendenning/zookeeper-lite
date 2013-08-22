package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.ReplyHeader;


public class IReplyHeader extends IRecord<ReplyHeader> implements Records.Header {

    public IReplyHeader() {
        this(new ReplyHeader());
    }

    public IReplyHeader(int xid, long zxid, KeeperException.Code code) {
        this(xid, zxid, code.intValue());
    }

    public IReplyHeader(int xid, long zxid, int err) {
        this(new ReplyHeader(xid, zxid, err));
    }

    public IReplyHeader(ReplyHeader record) {
        super(record);
    }
    
    public int getXid() {
        return record.getXid();
    }
    
    public long getZxid() {
        return record.getZxid();
    }
    
    public int getErr() {
        return record.getErr();
    }
}