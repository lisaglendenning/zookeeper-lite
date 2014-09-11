package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.InputArchive;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.MoreObjects;
import com.google.common.base.Supplier;


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
    
    public static Stat asStat(Records.ZNodeStatGetter source) {
        if (source instanceof IStat) {
            return ((IStat) source).record;
        } else {
            return new Stat(
                source.getCzxid(),
                source.getMzxid(),
                source.getCtime(),
                source.getMtime(),
                source.getVersion(),
                source.getCversion(),
                source.getAversion(),
                source.getEphemeralOwner(),
                source.getDataLength(),
                source.getNumChildren(),
                source.getPzxid());
        }
    }
    
    public static class CreateStat implements Records.CreateStatGetter {

        public static long ephemeralOwnerNone() {
            return 0L;
        }

        public static CreateStat nonEphemeral(long czxid) {
            return nonEphemeral(czxid, Stats.getTime());
        }

        public static CreateStat nonEphemeral(long czxid, long time) {
            return of(czxid, time, ephemeralOwnerNone());
        }

        public static CreateStat ephemeral(long czxid, long ephemeralOwner) {
            return of(czxid, Stats.getTime(), ephemeralOwner);
        }
        
        public static CreateStat of(long czxid, long ctime, long ephemeralOwner) {
            return new CreateStat(czxid, ctime, ephemeralOwner);
        }

        public static CreateStat copyOf(Records.CreateStatGetter value) {
            if (value instanceof CreateStat) {
                return (CreateStat) value;
            } else {
                return of(value.getCzxid(), value.getCtime(), value.getEphemeralOwner());
            }
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
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("czxid", getCzxid())
                    .add("ctime", getCtime())
                    .add("ephemeral", getEphemeralOwner())
                    .toString();
        }
    }
    
    public static class DataStat implements Records.DataStatSetter {

        public static DataStat initialVersion(long mzxid) {
            return of(mzxid, getTime(), Stats.initialVersion());
        }

        public static DataStat initialVersion(long mzxid, long mtime) {
            return of(mzxid, mtime, Stats.initialVersion());
        }
        
        public static DataStat of(long mzxid, long mtime, int version) {
            return new DataStat(mzxid, mtime, version);
        }

        public static DataStat copyOf(Records.DataStatGetter value) {
            return of(value.getMzxid(), value.getMtime(), value.getVersion());
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
        
        public void set(Records.DataStatGetter value) {
            setMzxid(value.getMzxid());
            setMtime(value.getMtime());
            setVersion(value.getVersion());
        }

        public int getAndIncrement(long mzxid, long mtime) {
            int prev = this.version;
            this.version = prev + 1;
            this.mtime = mtime;
            this.mzxid = mzxid;
            return prev;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("mzxid", getMzxid())
                    .add("mtime", getMtime())
                    .add("version", getVersion())
                    .toString();
        }
    }
    
    public static class ChildrenStat implements Records.ChildrenStatSetter {

        public static ChildrenStat initialVersion(long pzxid) {
            return of(pzxid, Stats.initialVersion());
        }
        
        public static ChildrenStat of(long pzxid, int cversion) {
            return new ChildrenStat(pzxid, cversion);
        }

        public static ChildrenStat copyOf(Records.ChildrenStatGetter value) {
            return of(value.getPzxid(), value.getCversion());
        }
        
        private long pzxid;
        private int cversion;
        
        public ChildrenStat(long pzxid, int cversion) {
            super();
            this.pzxid = pzxid;
            this.cversion = cversion;
        }

        @Override
        public int getCversion() {
            return cversion;
        }

        @Override
        public long getPzxid() {
            return pzxid;
        }

        @Override
        public void setCversion(int cversion) {
            this.cversion = cversion;
        }

        @Override
        public void setPzxid(long pzxid) {
            this.pzxid = pzxid;
        }
        
        public void set(Records.ChildrenStatGetter value) {
            setPzxid(value.getPzxid());
            setCversion(value.getCversion());
        }
        
        public boolean compareVersion(int cversion) {
            return Stats.compareVersion(cversion, getCversion());
        }

        public int getAndIncrement(long pzxid) {
            int prev = this.cversion;
            this.cversion = prev + 1;
            this.pzxid = pzxid;
            return prev;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("pzxid", getPzxid())
                    .add("cversion", getCversion())
                    .toString();
        }
    }
    
    public static class CompositeStatPersistedGetter implements Records.StatPersistedGetter {

        public static CompositeStatPersistedGetter of(
                Records.CreateStatGetter createStat, 
                Records.DataStatGetter dataStat, 
                Records.AclStatGetter aclStat,
                Records.ChildrenStatGetter childrenStat) {
            return new CompositeStatPersistedGetter(createStat, dataStat, aclStat, childrenStat);
        }
        
        protected final Records.CreateStatGetter createStat;
        protected final Records.DataStatGetter dataStat;
        protected final Records.AclStatGetter aclStat;
        protected final Records.ChildrenStatGetter childrenStat;
        
        public CompositeStatPersistedGetter(
                Records.CreateStatGetter createStat, 
                Records.DataStatGetter dataStat, 
                Records.AclStatGetter aclStat,
                Records.ChildrenStatGetter childrenStat) {
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

        @Override
        public String toString() {
            return Records.toBeanString(this);
        }
    }
    

    public static class CompositeStatGetter extends CompositeStatPersistedGetter implements Records.ZNodeStatGetter {

        public static CompositeStatGetter of(
                Records.StatPersistedGetter persisted,
                int dataLength,
                int numChildren) {
            return of(
                    persisted, persisted, persisted, persisted, dataLength, numChildren);
        }
        
        public static CompositeStatGetter of(
                Records.CreateStatGetter createStat, 
                Records.DataStatGetter dataStat, 
                Records.AclStatGetter aclStat,
                Records.ChildrenStatGetter childrenStat,
                int dataLength,
                int numChildren) {
            return new CompositeStatGetter(
                    createStat, dataStat, aclStat, childrenStat, dataLength, numChildren);
        }
        
        protected final int dataLength;
        protected final int numChildren;
        
        public CompositeStatGetter(
                Records.CreateStatGetter createStat,
                Records.DataStatGetter dataStat, 
                Records.AclStatGetter aclStat,
                Records.ChildrenStatGetter childrenStat,
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

        @Override
        public String toString() {
            return Records.toBeanString(this);
        }
    }
    
    public static class ImmutableStat extends Stat implements Records.ZNodeStatGetter {
        
        public static ImmutableStat uninitialized() {
            return Holder.ZEROS.get();
        }
        
        public static ImmutableStat copyOf(Records.ZNodeStatGetter stat) {
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
        
        public static enum Holder implements Supplier<ImmutableStat> {
            ZEROS(new ImmutableStat());
            
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
