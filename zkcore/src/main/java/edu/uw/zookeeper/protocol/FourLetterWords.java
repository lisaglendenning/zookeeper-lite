package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.ZNodePath;

public abstract class FourLetterWords {
    
    public static final class Mntr implements Iterable<Map.Entry<Mntr.MntrValueType<?>, Object>> {

        public static enum MntrServerState {
            STANDALONE, READ_ONLY, LEADER, FOLLOWER, OBSERVER;
            
            public static MntrServerState fromString(String str) {
                for (MntrServerState value: values()) {
                    if (value.toString().equals(str)) {
                        return value;
                    }
                }
                return null;
            }
            
            @Override
            public String toString() {
                return name().replace('_', '-').toLowerCase();
            }
        }
        
        public static enum MntrKey {
            ZK_VERSION(String.class),
            ZK_AVG_LATENCY(Long.class),
            ZK_MAX_LATENCY(Long.class),
            ZK_MIN_LATENCY(Long.class),
            ZK_PACKETS_RECEIVED(Long.class),
            ZK_PACKETS_SENT(Long.class),
            ZK_NUM_ALIVE_CONNECTIONS(Integer.class),
            ZK_OUTSTANDING_REQUESTS(Long.class),
            ZK_SERVER_STATE(MntrServerState.class),
            ZK_ZNODE_COUNT(Integer.class),
            ZK_WATCH_COUNT(Integer.class),
            ZK_EPHEMERALS_COUNT(Integer.class),
            ZK_APPROXIMATE_DATA_SIZE(Long.class),
            
            // Unix
            ZK_OPEN_FILE_DESCRIPTOR_COUNT(Integer.class),
            ZK_MAX_FILE_DESCRIPTOR_COUNT(Integer.class),
            
            // Leader
            ZK_FOLLOWERS(Integer.class),
            ZK_SYNCED_FOLLOWERS(Integer.class),
            ZK_PENDING_SYNCS(Integer.class);
            
            public static MntrKey fromString(String name) {
                return valueOf(name.toUpperCase());
            }
            
            private final Class<?> type;
            
            private MntrKey(Class<?> type) {
                this.type = type;
            }
            
            public Class<?> type() {
                return type;
            }
    
            @Override
            public String toString() {
                return name().toLowerCase();
            }
        }
        
        public static final class MntrValueType<V> extends AbstractPair<MntrKey,Class<V>> {
            
            private MntrValueType(MntrKey key, Class<V> type) {
                super(key, type);
            }
            
            public MntrKey getKey() {
                return first;
            }
            public Class<V> getType() {
                return second;
            }
        }
        
        public static <V> MntrValueType<V> getMntrValueType(MntrKey key, Class<V> type) {
            checkArgument(type == key.type());
            return new MntrValueType<V>(key, type);
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public static MntrValueType<?> getMntrValueType(MntrKey key) {
            return new MntrValueType(key, key.type());
        }
        
        public static Mntr fromString(String response) throws IOException {
            Properties properties = new Properties();
            properties.load(new StringReader(response));
            return fromProperties(properties);
        }
        
        public static Mntr fromProperties(Properties properties) {
            ImmutableMap.Builder<MntrValueType<?>, Object> values = ImmutableMap.builder();
            for (Map.Entry<Object,Object> property: properties.entrySet()) {
                MntrKey key = MntrKey.fromString(String.valueOf(property.getKey()));
                checkArgument(key != null);
                Class<?> type = key.type();
                Object value;
                if (type == String.class) {
                    value = String.valueOf(property.getValue());
                } else if (type == Integer.class) {
                    value = Integer.valueOf(String.valueOf(property.getValue()));
                } else if (type == Long.class) {
                    value = Long.valueOf(String.valueOf(property.getValue()));
                } else if (type == MntrServerState.class) {
                    value = MntrServerState.fromString(String.valueOf(property.getValue()));
                } else {
                    throw new AssertionError();
                }
                values.put(getMntrValueType(key), value);
            }
            return new Mntr(values.build());
        }
    
        private final ImmutableMap<MntrValueType<?>, Object> delegate;
        
        private Mntr(ImmutableMap<MntrValueType<?>, Object> delegate) {
            this.delegate = delegate;
        }
        
        @SuppressWarnings("unchecked")
        public <V> V getValue(MntrValueType<V> type) {
            return (V) delegate.get(type);
        }
        
        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public Iterator<Map.Entry<MntrValueType<?>, Object>> iterator() {
            return delegate.entrySet().iterator();
        }
    }
    
    public static abstract class DetailedWatches<K,V> implements Iterable<Map.Entry<K,Collection<V>>> {
        protected static Splitter LINE_SPLITTER = 
                Splitter.on(CharMatcher.anyOf("\r\n"))
                .omitEmptyStrings();
        
        protected static Pattern VALUE_LINE_PATTERN = Pattern.compile("^\t(.+)$");
        protected static Pattern SESSION_LINE_PATTERN = Pattern.compile("^0x(.+)$");
        
        protected static Function<String,Long> STRING_TO_SESSION = new Function<String,Long>() {
            @Override
            public Long apply(String input) {
                return Long.valueOf(SESSION_LINE_PATTERN.matcher(input).group(1), 16);
            }
        };
        
        protected static Function<String,ZNodePath> STRING_TO_PATH = new Function<String,ZNodePath>() {
            @Override
            public ZNodePath apply(String input) {
                return ZNodePath.fromString(input);
            }
        };
        
        protected static <K,V> ImmutableSetMultimap<K,V> fromLines(
                Iterator<String> lines, 
                Function<String,K> toKey,
                Function<String,V> toValue) {
            ImmutableSetMultimap.Builder<K, V> values = ImmutableSetMultimap.builder();
            K key = null;
            while (lines.hasNext()) {
                String line = lines.next();
                Matcher valueMatcher;
                if ((key != null) && (valueMatcher = VALUE_LINE_PATTERN.matcher(line)).matches()) {
                    V value = toValue.apply(valueMatcher.group(1));
                    values.put(key, value);
                } else {
                    key = toKey.apply(line);
                }
            }
            return values.build();
        }
        
        protected final ImmutableSetMultimap<K,V> delegate;
        
        protected DetailedWatches(
                ImmutableSetMultimap<K,V> delegate) {
            this.delegate = delegate;
        }
        
        public Set<K> keySet() {
            return delegate.keySet();
        }
        
        public ImmutableSet<V> getValues(K key) {
            return delegate.get(key);
        }
        
        public ImmutableSetMultimap<K,V> asMultimap() {
            return delegate;
        }
        
        @Override
        public Iterator<Map.Entry<K,Collection<V>>> iterator() {
            return delegate.asMap().entrySet().iterator();
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(delegate).toString();
        }
        
        @Override
        public int hashCode() {
            return delegate.hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            if ((obj == null) || (obj.getClass() != getClass())) {
                return false;
            }
            return Objects.equal(delegate, ((DetailedWatches<?,?>) obj).delegate);
        }
    }
    
    public static final class Wchc extends DetailedWatches<Long,ZNodePath> {

        public static Wchc fromString(
                String response) {
            return fromLines(LINE_SPLITTER.split(response).iterator());
        }
        
        public static Wchc fromLines(
                Iterator<String> lines) {
            return fromMultimap(fromLines(lines, STRING_TO_SESSION, STRING_TO_PATH));
        }
        
        public static Wchc fromMultimap(ImmutableSetMultimap<Long,ZNodePath> watches) {
            return new Wchc(watches);
        }

        private Wchc(ImmutableSetMultimap<Long,ZNodePath> delegate) {
            super(delegate);
        }
    }
    
    public static final class Wchp extends DetailedWatches<ZNodePath,Long> {

        public static Wchp fromString(
                String response) {
            return fromLines(LINE_SPLITTER.split(response).iterator());
        }
        
        public static Wchp fromLines(
                Iterator<String> lines) {
            return fromMultimap(fromLines(lines, STRING_TO_PATH, STRING_TO_SESSION));
        }
        
        public static Wchp fromMultimap(ImmutableSetMultimap<ZNodePath, Long> watches) {
            return new Wchp(watches);
        }

        private Wchp(ImmutableSetMultimap<ZNodePath, Long> delegate) {
            super(delegate);
        }
    }
    
    private FourLetterWords() {}
}
