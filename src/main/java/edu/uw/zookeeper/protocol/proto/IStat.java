package edu.uw.zookeeper.protocol.proto;


import org.apache.zookeeper.data.Stat;

public class IStat extends IRecord<Stat> implements Records.StatHolderInterface {

    public IStat() {
        this(new Stat());
    }

    public IStat(long czxid, long mzxid, long ctime, long mtime, int version,
            int cversion, int aversion, long ephemeralOwner, int dataLength,
            int numChildren, long pzxid) {
        this(new Stat(czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner,
                dataLength, numChildren, pzxid));
    }

    public IStat(Stat record) {
        super(record);
    }

    @Override
    public int getDataLength() {
        return get().getDataLength();
    }

    @Override
    public int getNumChildren() {
        return get().getNumChildren();
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
