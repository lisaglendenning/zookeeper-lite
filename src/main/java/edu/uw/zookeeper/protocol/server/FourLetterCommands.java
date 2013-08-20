package edu.uw.zookeeper.protocol.server;

import io.netty.buffer.ByteBuf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;

import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterWord;
import edu.uw.zookeeper.protocol.FourLetterResponse;

/**
 * TODO
 */
public abstract class FourLetterCommands {

    public static Processor<FourLetterRequest, FourLetterResponse> getInstance(FourLetterWord word) {
        Class<? extends Processor<FourLetterRequest, FourLetterResponse>> type = Types.TYPES.get(word);
        if (type == null) {
            throw new UnsupportedOperationException(String.valueOf(word));
        }
        if (type.isEnum()) {
            return Iterators.getOnlyElement(Iterators.forArray(type.getEnumConstants()));
        } else {
            try {
                return type.newInstance();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
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

        protected static final FourLetterResponse RESPONSE = FourLetterResponse.create(IMOK);
        
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
            return FourLetterResponse.create(new StringBuilder().append(trace).toString());
        }
    }

    @FourLetterCommand(FourLetterWord.STMK)
    public static class StmkCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            ByteBuf arg = input.second().get();
            long trace = arg.getLong(arg.readerIndex());
            return FourLetterResponse.create(new StringBuilder().append(trace).toString());
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
            return FourLetterResponse.create(String.format(
                    FORMAT, Joiner.on('\n').withKeyValueSeparator("=").join(values)));
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
            return FourLetterResponse.create(
                    Joiner.on('\n').withKeyValueSeparator("=").join(values));
        }
    }

    @FourLetterCommand(FourLetterWord.SRST)
    public static class SrstCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        public static final String RESULT = "Server stats reset.\n";

        protected static final FourLetterResponse RESPONSE = FourLetterResponse.create(RESULT);
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return RESPONSE;
        }
    }
    
    @FourLetterCommand(FourLetterWord.CRST)
    public static class CrstCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        public static final String RESULT = "Connection stats reset.\n";

        protected static final FourLetterResponse RESPONSE = FourLetterResponse.create(RESULT);
        
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
            return FourLetterResponse.create(
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
            return FourLetterResponse.create(
                    String.format(
                            FORMAT, ZOOKEEPER_VERSION, "", "", "", 0));
        }
    }

    @FourLetterCommand(FourLetterWord.STAT)
    public static class StatCommand extends SrvrCommand {

        public static final String STAT_FORMAT = "Clients:%n%s%n";
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return FourLetterResponse.create(
                    String.format(
                            FORMAT, ZOOKEEPER_VERSION, "",
                            String.format(STAT_FORMAT, ""), "", 0));
        }
    }

    @FourLetterCommand(FourLetterWord.CONS)
    public static class ConsCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return FourLetterResponse.create("\n");
        }
    }
    
    public static class WatchCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            return FourLetterResponse.create("\n");
        }
    }
    
    @FourLetterCommand(FourLetterWord.WCHC)
    public static class WchcCommand extends WatchCommand {
    }

    @FourLetterCommand(FourLetterWord.WCHP)
    public static class WchpCommand extends WatchCommand {
    }

    @FourLetterCommand(FourLetterWord.WCHS)
    public static class WchsCommand extends WatchCommand {
    }

    @FourLetterCommand(FourLetterWord.MNTR)
    public static class MntrCommand implements Processor<FourLetterRequest, FourLetterResponse> {

        public static enum ZkServerState {
            STANDALONE, READ_ONLY, LEADER, FOLLOWER, OBSERVER;
            
            @Override
            public String toString() {
                return name().replace('_', '-').toLowerCase();
            }
        }
        
        public static enum MntrKey {
            VERSION,
            AVG_LATENCY,
            MAX_LATENCY,
            MIN_LATENCY,
            PACKETS_RECEIVED,
            PACKETS_SENT,
            NUM_ALIVE_CONNECTIONS,
            OUTSTANDING_REQUESTS,
            SERVER_STATE,
            ZNODE_COUNT,
            WATCH_COUNT,
            EPHEMERALS_COUNT,
            APPROXIMATE_DATA_SIZE;

            public static Map<MntrKey, String> getValues() {
                ImmutableSortedMap.Builder<MntrKey, String> builder = ImmutableSortedMap.naturalOrder();
                for (MntrKey k: values()) {
                    builder.put(k, k.value());
                }
                return builder.build();
            }
            
            public String value() {
                switch (this) {
                case VERSION:
                    return ZOOKEEPER_VERSION;
                case AVG_LATENCY:
                case MAX_LATENCY:
                case MIN_LATENCY:
                case APPROXIMATE_DATA_SIZE:
                case PACKETS_RECEIVED:
                case PACKETS_SENT:
                case OUTSTANDING_REQUESTS:
                    return String.valueOf(0L);
                case NUM_ALIVE_CONNECTIONS:
                case ZNODE_COUNT:
                case EPHEMERALS_COUNT:
                case WATCH_COUNT:
                    return String.valueOf(0);
                case SERVER_STATE:
                    return ZkServerState.STANDALONE.toString();
                }
                return "";
            }

            @Override
            public String toString() {
                return name().toLowerCase();
            }
        }
        
        public static enum MntrUnixKey {
            OPEN_FILE_DESCRIPTOR_COUNT,
            MAX_FILE_DESCRIPTOR_COUNT;

            @Override
            public String toString() {
                return name().toLowerCase();
            }
        }
        
        public static enum MntrLeaderKey {
            FOLLOWERS,
            SYNCED_FOLLOWERS,
            PENDING_SYNCS;

            @Override
            public String toString() {
                return name().toLowerCase();
            }
        }
        
        @Override
        public FourLetterResponse apply(FourLetterRequest input) {
            Map<MntrKey, String> values = MntrKey.getValues();
            return FourLetterResponse.create(
                    Joiner.on('\n').withKeyValueSeparator("=").join(values));
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
            return FourLetterResponse.create(IsRoValue.RW.toString());
        }
    }
}
