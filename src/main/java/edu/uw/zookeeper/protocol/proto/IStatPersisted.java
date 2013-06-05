package edu.uw.zookeeper.protocol.proto;

import edu.uw.zookeeper.protocol.Records;

import org.apache.zookeeper.data.StatPersisted;

public class IStatPersisted extends StatPersisted implements Records.StatPersistedRecord {

    public IStatPersisted() {
        super();
    }

    public IStatPersisted(long czxid, long mzxid, long ctime, long mtime,
            int version, int cversion, int aversion, long ephemeralOwner,
            long pzxid) {
        super(czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner,
                pzxid);
    }
}
