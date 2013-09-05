package edu.uw.zookeeper.protocol.proto;


import org.apache.zookeeper.data.Stat;

public class IStat extends IRecord<Stat> implements Records.ZNodeStatGetter {

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
        return record.getDataLength();
    }

    @Override
    public int getNumChildren() {
        return record.getNumChildren();
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
