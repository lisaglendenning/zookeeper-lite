package edu.uw.zookeeper.protocol.proto;


import org.apache.zookeeper.data.StatPersisted;

public class IStatPersisted extends IRecord<StatPersisted> implements Records.StatPersistedHolder {

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
        return get().getCzxid();
    }

    @Override
    public long getCtime() {
        return get().getCtime();
    }

    @Override
    public long getEphemeralOwner() {
        return get().getEphemeralOwner();
    }

    @Override
    public long getMzxid() {
        return get().getMzxid();
    }

    @Override
    public long getMtime() {
        return get().getMtime();
    }

    @Override
    public int getVersion() {
        return get().getVersion();
    }

    @Override
    public int getAversion() {
        return get().getAversion();
    }

    @Override
    public int getCversion() {
        return get().getCversion();
    }

    @Override
    public long getPzxid() {
        return get().getPzxid();
    }
}
