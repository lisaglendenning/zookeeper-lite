package edu.uw.zookeeper.data;

import org.apache.jute.InputArchive;
import org.apache.zookeeper.data.Stat;

import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.Records.AclStatHolder;
import edu.uw.zookeeper.protocol.Records.ChildrenStatRecord;
import edu.uw.zookeeper.protocol.Records.CreateStatHolder;
import edu.uw.zookeeper.protocol.Records.DataStatHolder;
import edu.uw.zookeeper.protocol.proto.IStat;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Singleton;



public class Stats {

    public static int VERSION_ANY = -1;
    
    public static boolean compareVersion(int expected, int actual) {
        return ((VERSION_ANY == expected) || (actual == expected));
    }

    public static long getTime() {
        return System.currentTimeMillis();
    }
    
    public static int initialVersion() {
        return 0;
    }
    
    public static Stat asStat(Records.StatHolderInterface source) {
        if (source instanceof Stat) {
            return (Stat)source;
        }
        return copy(source, new IStat());
    }
    
    public static <T extends Records.StatRecordInterface> T copy(Records.StatHolderInterface source, T dest) {
        dest.setAversion(source.getAversion());
        dest.setCtime(source.getCtime());
        dest.setCversion(source.getCversion());
        dest.setCzxid(source.getCzxid());
        dest.setDataLength(source.getDataLength());
        dest.setEphemeralOwner(source.getEphemeralOwner());
        dest.setMtime(source.getMtime());
        dest.setMzxid(source.getMzxid());
        dest.setNumChildren(source.getNumChildren());
        dest.setPzxid(source.getPzxid());
        dest.setVersion(source.getVersion());
        return dest;
    }
    
    public static class CreateStat implements Records.CreateStatHolder {

        public static long ephemeralOwnerNone() {
            return 0;
        }

        public static CreateStat nonEphemeral(long czxid) {
            return of(czxid, Stats.getTime(), ephemeralOwnerNone());
        }

        public static CreateStat ephemeral(long czxid, long ephemeralOwner) {
            return of(czxid, Stats.getTime(), ephemeralOwner);
        }
        
        public static CreateStat of(long czxid, long ctime, long ephemeralOwner) {
            return new CreateStat(czxid, ctime, ephemeralOwner);
        }
        
        private final long czxid;
        private final long ctime;
        private final long ephemeralOwner;
        
        public CreateStat(long czxid, long ctime, long ephemeralOwner) {
            this.czxid = czxid;
            this.ctime = ctime;
            this.ephemeralOwner = ephemeralOwner;
        }
        
        @Override
        public long getCzxid() {
            return czxid;
        }

        @Override
        public long getCtime() {
            return ctime;
        }

        @Override
        public long getEphemeralOwner() {
            return ephemeralOwner;
        }
        
        public boolean isEphemeral() {
            return ephemeralOwnerNone() != getEphemeralOwner();
        }
    }
    
    public static class DataStat implements Records.DataStatRecord {

        public static DataStat newInstance(long mzxid) {
            return of(mzxid, getTime(), initialVersion());
        }

        public static DataStat newInstance(long mzxid, long mtime) {
            return of(mzxid, mtime, initialVersion());
        }
        
        public static DataStat of(long mzxid, long mtime, int version) {
            return new DataStat(mzxid, mtime, version);
        }
        
        private long mzxid;
        private long mtime;
        private int version;
        
        public DataStat(long mzxid, long mtime, int version) {
            super();
            this.mzxid = mzxid;
            this.mtime = mtime;
            this.version = version;
        }

        @Override
        public long getMzxid() {
            return mzxid;
        }

        @Override
        public long getMtime() {
            return mtime;
        }

        @Override
        public int getVersion() {
            return version;
        }

        @Override
        public void setMzxid(long mzxid) {
            this.mzxid = mzxid;
        }

        @Override
        public void setMtime(long mtime) {
            this.mtime = mtime;
        }

        @Override
        public void setVersion(int version) {
            this.version = version;
        }
        
        public boolean compareVersion(int version) {
            return Stats.compareVersion(version, getVersion());
        }

        public int getAndIncrement(long mzxid, long mtime) {
            int prev = this.version;
            this.version = prev + 1;
            this.mtime = mtime;
            this.mzxid = mzxid;
            return prev;
        }
    }
    
    public static class ChildrenStat implements Records.ChildrenStatRecord {

        public static ChildrenStat newInstance(long pzxid) {
            return of(pzxid, initialVersion());
        }
        
        public static ChildrenStat of(long pzxid, int cversion) {
            return new ChildrenStat(pzxid, cversion);
        }
        
        private Pair<Integer, Long> version;
        
        public ChildrenStat(long pzxid, int cversion) {
            super();
            this.version = Pair.create(cversion, pzxid);
        }

        @Override
        public int getCversion() {
            return version.first();
        }

        @Override
        public long getPzxid() {
            return version.second();
        }

        @Override
        public void setCversion(int cversion) {
            this.version = Pair.create(cversion, version.second());
        }

        @Override
        public void setPzxid(long pzxid) {
            this.version = Pair.create(version.first(), pzxid);
        }

        public Pair<Integer, Long> getAndIncrement(long pzxid) {
            return getAndSet(getCversion() + 1, pzxid);
        }
        
        public Pair<Integer, Long> getAndSet(int cversion, long pzxid) {
            Pair<Integer, Long> prev = this.version;
            this.version = Pair.create(cversion, pzxid);
            return prev;
        }
    }
    
    public static class CompositeStatPersistedHolder implements Records.StatPersistedHolder {

        private final Records.CreateStatHolder createStat;
        private final Records.DataStatHolder dataStat;
        private final Records.AclStatHolder aclStat;
        private final Records.ChildrenStatRecord childrenStat;
        
        public CompositeStatPersistedHolder(
                Records.CreateStatHolder createStat, 
                Records.DataStatHolder dataStat, 
                Records.AclStatHolder aclStat,
                Records.ChildrenStatRecord childrenStat) {
            this.createStat = createStat;
            this.dataStat = dataStat;
            this.aclStat = aclStat;
            this.childrenStat = childrenStat;
        }
        
        @Override
        public long getCzxid() {
            return createStat.getCzxid();
        }

        @Override
        public long getCtime() {
            return createStat.getCtime();
        }

        @Override
        public long getEphemeralOwner() {
            return createStat.getEphemeralOwner();
        }

        @Override
        public long getMzxid() {
            return dataStat.getMzxid();
        }

        @Override
        public long getMtime() {
            return dataStat.getMtime();
        }

        @Override
        public int getVersion() {
            return dataStat.getVersion();
        }

        @Override
        public int getAversion() {
            return aclStat.getAversion();
        }

        @Override
        public int getCversion() {
            return childrenStat.getCversion();
        }

        @Override
        public long getPzxid() {
            return childrenStat.getPzxid();
        }
    }
    

    public static class CompositeStatHolder extends CompositeStatPersistedHolder implements Records.StatHolderInterface {

        private final int dataLength;
        private final int numChildren;
        
        public CompositeStatHolder(CreateStatHolder createStat,
                DataStatHolder dataStat, AclStatHolder aclStat,
                ChildrenStatRecord childrenStat,
                int dataLength,
                int numChildren) {
            super(createStat, dataStat, aclStat, childrenStat);
            this.dataLength = dataLength;
            this.numChildren = numChildren;
        }

        @Override
        public int getDataLength() {
            return dataLength;
        }

        @Override
        public int getNumChildren() {
            return numChildren;
        }
    }
    
    
    public static class ImmutableStat extends IStat {
        
        public static ImmutableStat uninitialized() {
            return Holder.INSTANCE.get();
        }
        
        public static ImmutableStat copyOf(Records.StatHolderInterface stat) {
            return of(stat.getCzxid(), stat.getMzxid(), stat.getCtime(),
                    stat.getMtime(), stat.getVersion(), stat.getCversion(),
                    stat.getAversion(), stat.getEphemeralOwner(), stat.getDataLength(),
                    stat.getNumChildren(), stat.getPzxid());
        }

        public static ImmutableStat of(long czxid, long mzxid, long ctime, long mtime,
                int version, int cversion, int aversion, long ephemeralOwner,
                int dataLength, int numChildren, long pzxid) {
            return new ImmutableStat(
                    czxid, mzxid, ctime, mtime, version, cversion, aversion,
                    ephemeralOwner, dataLength, numChildren, pzxid);
        }
        
        public static enum Holder implements Singleton<ImmutableStat> {
            INSTANCE(new ImmutableStat());
            
            private final ImmutableStat instance;
            
            private Holder(ImmutableStat instance) {
                this.instance = instance;
            }

            @Override
            public ImmutableStat get() {
                return instance;
            }
        }
        
        public ImmutableStat() {
            this(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        public ImmutableStat(long czxid, long mzxid, long ctime, long mtime,
                int version, int cversion, int aversion, long ephemeralOwner,
                int dataLength, int numChildren, long pzxid) {
            super(czxid, mzxid, ctime, mtime, version, cversion, aversion,
                    ephemeralOwner, dataLength, numChildren, pzxid);
        }

        @Override
        public void setCzxid(long m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMzxid(long m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCtime(long m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMtime(long m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setVersion(int m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCversion(int m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAversion(int m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setEphemeralOwner(long m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDataLength(int m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNumChildren(int m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setPzxid(long m_) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deserialize(InputArchive a_, String tag)
                throws java.io.IOException {
            throw new UnsupportedOperationException();
        }
    }
}
