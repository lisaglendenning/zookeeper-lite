package edu.uw.zookeeper.protocol.proto;


import org.apache.zookeeper.data.StatPersisted;

public class IStatPersisted extends IRecord<StatPersisted> implements Records.StatPersistedGetter {

    public IStatPersisted() {
        this(new StatPersisted());
    }

    public IStatPersisted(long czxid, long mzxid, long ctime, long mtime,
            int version, int cversion, int aversion, long ephemeralOwner,
            long pzxid) {
        this(new StatPersisted(czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner,
                pzxid));
    }

    public IStatPersisted(StatPersisted record) {
        super(record);
    }

    @Override
    public long getCzxid() {
        return record.getCzxid();
    }

    @Override
    public long getCtime() {
        return record.getCtime();
    }

    @Override
    public long getEphemeralOwner() {
        return record.getEphemeralOwner();
    }

    @Override
    public long getMzxid() {
        return record.getMzxid();
    }

    @Override
    public long getMtime() {
        return record.getMtime();
    }

    @Override
    public int getVersion() {
        return record.getVersion();
    }

    @Override
    public int getAversion() {
        return record.getAversion();
    }

    @Override
    public int getCversion() {
        return record.getCversion();
    }

    @Override
    public long getPzxid() {
        return record.getPzxid();
    }
}
