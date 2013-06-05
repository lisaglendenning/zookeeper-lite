package edu.uw.zookeeper.protocol.proto;

import edu.uw.zookeeper.protocol.Records;

import org.apache.zookeeper.data.Stat;

public class IStat extends Stat implements Records.StatRecordInterface {

    public IStat() {
        super();
    }

    public IStat(long czxid, long mzxid, long ctime, long mtime, int version,
            int cversion, int aversion, long ephemeralOwner, int dataLength,
            int numChildren, long pzxid) {
        super(czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner,
                dataLength, numChildren, pzxid);
    }
}
