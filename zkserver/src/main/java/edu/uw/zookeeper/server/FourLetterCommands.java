package edu.uw.zookeeper.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;

import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterWord;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.FourLetterWords;
import edu.uw.zookeeper.protocol.FourLetterWords.Mntr.MntrServerState;
import edu.uw.zookeeper.protocol.FourLetterWords.Wchs;

/**
 * TODO
 */
public abstract class FourLetterCommands {

    @SuppressWarnings("unchecked")
    public static Processor<FourLetterRequest, FourLetterResponse> create(FourLetterWord word, Object...args) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        Class<? extends Processor<FourLetterRequest, FourLetterResponse>> type = Types.TYPES.get(word);
        if (type == null) {
            throw new UnsupportedOperationException(String.valueOf(word));
        }
        if (type.isEnum()) {
            return Iterators.getOnlyElement(Iterators.forArray(type.getEnumConstants()));
        } else {
            Constructor<?>[] ctors = type.getConstructors();
            for (Constructor<?> ctor: ctors) {
                if (ctor.getParameterTypes().length == args.length) {
                    return (Processor<FourLetterRequest, FourLetterResponse>) ctor.newInstance(args);
                }
            }
            for (Constructor<?> ctor: ctors) {
                if (ctor.getParameterTypes().length == 0) {
                    return (Processor<FourLetterRequest, FourLetterResponse>) ctor.newInstance();
                }
            }
            throw new IllegalArgumentException();
        }
    }
    
    protected static enum Types implements ParameterizedFactory<FourLetterWord, Class<? extends Processor<FourLetterRequest, FourLetterResponse>>> {
        TYPES(FourLetterCommands.class);
        
        private final Map<FourLetterWord, Class<? extends Processor<FourLetterRequest, FourLetterResponse>>> types;
        
        @SuppressWarnings("unchecked")
        private Types(Class<?> type) {
            ImmutableMap.Builder<FourLetterWord, Class<? extends Processor<FourLetterRequest, FourLetterResponse>>> builder = ImmutableMap.builder();
            for (Class<?> cls: FourLetterCommands.class.getClasses()) {
                FourLetterCommand annotation = cls.getAnnotation(FourLetterCommand.class);
                if (annotation != null) {
                    builder.put(annotation.value(), (Class<? extends Processor<FourLetterRequest, FourLetterResponse>>) cls);
                }
            }
            this.types = builder.build();
        }

        @Override
        public Class<? extends Processor<FourLetterRequest, FourLetterResponse>> get(
                FourLetterWord value) {
            return types.get(value);
        }
    }

    public static final String ZK_NOT_SERVING =
    "This ZooKeeper instance is not currently serving requests\n";

    public static String ZOOKEEPER_VERSION = "3.5.0";

    @FourLetterCommand(FourLetterWord.RUOK)
    public static enum RuokCommand implements Processor<FourLetterRequest, FourLetterResponse> {
        RUOK_COMMAND;
        
        public static final String IMOK = "imok";

        protected static final FourLetterResponse RESPONSE = FourLetterResponse.fromString(IMOK);
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return RESPONSE;
        }
    }

    @FourLetterCommand(FourLetterWord.GTMK)
    public static class GtmkCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            long trace = 0L;
            return FourLetterResponse.fromString(new StringBuilder().append(trace).toString());
        }
    }

    @FourLetterCommand(FourLetterWord.STMK)
    public static class StmkCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            ByteBuf arg = Unpooled.wrappedBuffer(input.second());
            long trace = arg.readLong();
            arg.release();
            return FourLetterResponse.fromString(new StringBuilder().append(trace).toString());
        }
    }

    @FourLetterCommand(FourLetterWord.ENVI)
    public static class EnviCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        public static String FORMAT = "Environment:%n%s";
        
        public static String DEFAULT_VALUE = "<NA>";
        
        public static enum EnvKey {
            ZOOKEEPER_VERSION,
            HOST_NAME,
            JAVA_VERSION,
            JAVA_VENDOR,
            JAVA_HOME,
            JAVA_CLASS_PATH,
            JAVA_LIBRARY_PATH,
            JAVA_IO_TMPDIR,
            JAVA_COMPILER,
            OS_NAME,
            OS_ARCH,
            OS_VERSION,
            USER_NAME,
            USER_HOME,
            USER_DIR;
            
            public static Map<EnvKey, String> getValues() {
                ImmutableSortedMap.Builder<EnvKey, String> builder = ImmutableSortedMap.naturalOrder();
                for (EnvKey k: values()) {
                    builder.put(k, k.value());
                }
                return builder.build();
            }
            
            public String value() {
                switch (this) {
                case ZOOKEEPER_VERSION:
                    return FourLetterCommands.ZOOKEEPER_VERSION;
                case HOST_NAME:
                    try {
                        return InetAddress.getLocalHost().getCanonicalHostName();
                    } catch (UnknownHostException e) {
                        return DEFAULT_VALUE;
                    }
                default:
                    return System.getProperty(toString(), DEFAULT_VALUE);
                }
            }
            
            @Override
            public String toString() {
                return name().toLowerCase().replace('_', '.');
            }
        }
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            Map<EnvKey, String> values = EnvKey.getValues();
            StringBuilder builder = Joiner.on('\n').withKeyValueSeparator("=").appendTo(new StringBuilder(), values);
            if (! values.isEmpty()) {
                builder.append('\n');
            }
            return FourLetterResponse.fromString(String.format(FORMAT, builder.toString()));
        }
    }

    @FourLetterCommand(FourLetterWord.CONF)
    public static class ConfCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        public static enum ConfKey {
            CLIENT_PORT, 
            DATA_DIR, 
            DATA_LOG_DIR, 
            TICK_TIME, 
            MAX_CLIENT_CNXNS, 
            MIN_SESSION_TIMEOUT, 
            MAX_SESSION_TIMEOUT, 
            SERVER_ID;

            public static Map<ConfKey, String> getValues() {
                ImmutableSortedMap.Builder<ConfKey, String> builder = ImmutableSortedMap.naturalOrder();
                for (ConfKey k: values()) {
                    builder.put(k, k.value());
                }
                return builder.build();
            }
            
            public String value() {
                switch (this) {
                case CLIENT_PORT:
                case TICK_TIME:
                case MAX_CLIENT_CNXNS:
                case MIN_SESSION_TIMEOUT:
                case MAX_SESSION_TIMEOUT:
                    return String.valueOf(0);
                case DATA_DIR:
                case DATA_LOG_DIR:
                    return "";
                case SERVER_ID:
                    return String.valueOf(0L);
                }
                return "";
            }

            @Override
            public String toString() {
                return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name());
            }
        }
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            Map<ConfKey, String> values = ConfKey.getValues();
            StringBuilder builder = Joiner.on('\n').withKeyValueSeparator("=").appendTo(new StringBuilder(), values);
            if (! values.isEmpty()) {
                builder.append('\n');
            }
            return FourLetterResponse.fromString(builder.toString());
        }
    }

    @FourLetterCommand(FourLetterWord.SRST)
    public static class SrstCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        public static final String RESULT = "Server stats reset.\n";

        protected static final FourLetterResponse RESPONSE = FourLetterResponse.fromString(RESULT);
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return RESPONSE;
        }
    }
    
    @FourLetterCommand(FourLetterWord.CRST)
    public static class CrstCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        public static final String RESULT = "Connection stats reset.\n";

        protected static final FourLetterResponse RESPONSE = FourLetterResponse.fromString(RESULT);
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return RESPONSE;
        }
    }

    @FourLetterCommand(FourLetterWord.DUMP)
    public static class DumpCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        public static final String FORMAT = "SessionTracker dump:%n%sephemeral nodes dump:%n%sConnections dump:%n%s";
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return FourLetterResponse.fromString(
                    String.format(
                            FORMAT, "", "", ""));
        }
    }
    
    @FourLetterCommand(FourLetterWord.SRVR)
    public static class SrvrCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        public static final String FORMAT = "Zookeeper version: %s%n%s%s%sNode count: %d%n";
        
        public static final String READ_ONLY = "READ-ONLY mode; serving only read-only clients";
        
        public static final String STAT_FORMAT = "Clients:%n%s%n";
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return FourLetterResponse.fromString(
                    String.format(
                            FORMAT, ZOOKEEPER_VERSION, "", "", "", 0));
        }
    }

    @FourLetterCommand(FourLetterWord.STAT)
    public static class StatCommand extends SrvrCommand {

        public static final String STAT_FORMAT = "Clients:%n%s%n";
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return FourLetterResponse.fromString(
                    String.format(
                            FORMAT, ZOOKEEPER_VERSION, "",
                            String.format(STAT_FORMAT, ""), "", 0));
        }
    }

    @FourLetterCommand(FourLetterWord.CONS)
    public static class ConsCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return FourLetterResponse.fromString("\n");
        }
    }
    
    public static abstract class WatchCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        protected final ReentrantReadWriteLock lock;
        protected final Watches dataWatches;
        protected final Watches childWatches;
        
        protected WatchCommand(
                ReentrantReadWriteLock lock,
                Watches dataWatches,
                Watches childWatches) {
            this.lock = lock;
            this.dataWatches = dataWatches;
            this.childWatches = childWatches;
        }
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            lock.readLock().lock();
            try {
                return FourLetterResponse.fromString(apply());
            } finally {
                lock.readLock().unlock();
            }
        }
        
        protected abstract String apply();
    }
    
    @FourLetterCommand(FourLetterWord.WCHC)
    public static class WchcCommand extends WatchCommand {

        public WchcCommand(
                SimpleServer.Builder<?> server) {
            this(server.getLock(), server.getDataWatches(), server.getChildWatches());
        }
        
        public WchcCommand(
                ReentrantReadWriteLock lock,
                Watches dataWatches,
                Watches childWatches) {
            super(lock, dataWatches, childWatches);
        }
        
        @Override
        protected String apply() {
            return FourLetterWords.Wchc.toString(dataWatches.bySession());
        }
    }

    @FourLetterCommand(FourLetterWord.WCHP)
    public static class WchpCommand extends WatchCommand {

        public WchpCommand(
                SimpleServer.Builder<?> server) {
            this(server.getLock(), server.getDataWatches(), server.getChildWatches());
        }
        
        public WchpCommand(
                ReentrantReadWriteLock lock,
                Watches dataWatches,
                Watches childWatches) {
            super(lock, dataWatches, childWatches);
        }
        
        @Override
        protected String apply() {
            return FourLetterWords.Wchp.toString(dataWatches.byPath());
        }
    }

    @FourLetterCommand(FourLetterWord.WCHS)
    public static class WchsCommand extends WatchCommand {

        public WchsCommand(
                SimpleServer.Builder<?> server) {
            this(server.getLock(), server.getDataWatches(), server.getChildWatches());
        }
        
        public WchsCommand(
                ReentrantReadWriteLock lock,
                Watches dataWatches,
                Watches childWatches) {
            super(lock, dataWatches, childWatches);
        }
        
        @Override
        protected String apply() {
            return FourLetterWords.Wchs.toString(
                    new Wchs(dataWatches.bySession().keySet().size(), 
                            dataWatches.byPath().keySet().size(), 
                            dataWatches.bySession().size()));
        }
    }

    @FourLetterCommand(FourLetterWord.MNTR)
    public static class MntrCommand implements Processor<FourLetterRequest, FourLetterResponse> {
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            ImmutableSortedMap.Builder<FourLetterWords.Mntr.MntrKey, String> properties = ImmutableSortedMap.naturalOrder();
            for (FourLetterWords.Mntr.MntrKey k: FourLetterWords.Mntr.MntrKey.values()) {
                Class<?> type = k.type();
                String v;
                if (type == String.class) {
                    v = "";
                } else if (type == Integer.class) {
                    v = String.valueOf(0);
                } else if (type == Long.class) {
                    v = String.valueOf(0L);
                } else if (type == MntrServerState.class) {
                    v = FourLetterWords.Mntr.MntrServerState.STANDALONE.toString();
                } else {
                    throw new AssertionError();
                }
                properties.put(k, v);
            }
            Map<FourLetterWords.Mntr.MntrKey, String> values = properties.build();
            StringBuilder builder = Joiner.on('\n').withKeyValueSeparator("\t").appendTo(new StringBuilder(), values);
            if (! values.isEmpty()) {
                builder.append('\n');
            }
            return FourLetterResponse.fromString(builder.toString());
        }
    }

    @FourLetterCommand(FourLetterWord.ISRO)
    public static class IsroCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        public static enum IsRoValue {
            NULL, RO, RW;
            
            @Override
            public String toString() {
                return name().toLowerCase();
            }
        }
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return FourLetterResponse.fromString(IsRoValue.RW.toString());
        }
    }
}
