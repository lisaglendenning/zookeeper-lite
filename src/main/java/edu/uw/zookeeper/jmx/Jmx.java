package edu.uw.zookeeper.jmx;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factory;

public abstract class Jmx {
    
    public static String FORMAT_REGEX = "%.";
    public static char WILDCARD = '*';
    
    public static String patternOf(String format) {
        return format.toString().replaceAll(FORMAT_REGEX, Character.toString(WILDCARD));
    }

    public static String listPatternOf(String name) {
        return name + ",*";
    }

    public abstract static class PathObjectName {

        public static char KEY_SEPARATOR = '=';
        public static char PROPERTY_SEPARATOR = ',';
        
        public static ObjectName of(ZNodeLabel.Path input) {
            return PathToObjectName.ZOOKEEPER_SERVICE.apply(input);
        }
        
        public static ZNodeLabel.Path of(ObjectName input) {
            return ObjectNameToPath.ZOOKEEPER_SERVICE.apply(input);
        }
        
        public static enum PathToObjectName implements Function<ZNodeLabel.Path, ObjectName> {
            ZOOKEEPER_SERVICE(Domain.ZOOKEEPER_SERVICE);
        
            public static String KEY_FORMAT = "name%d";
            public static String PROPERTY_FORMAT = KEY_FORMAT + KEY_SEPARATOR + "%s";
            public static Joiner JOINER = Joiner.on(PROPERTY_SEPARATOR).useForNull("*");
            
            private final Domain domain;
            
            private PathToObjectName(Domain domain) {
                this.domain = domain;
            }
            
            @Override
            public ObjectName apply(ZNodeLabel.Path input) {
                List<String> properties = Lists.newLinkedList();
                int index = 0;
                for (ZNodeLabel.Component component: input) {
                    properties.add(String.format(PROPERTY_FORMAT, index, component.toString()));
                    index += 1;
                }
                return domain.apply(JOINER.join(properties));
            }
        }
        
        public static enum ObjectNameToPath implements Function<ObjectName, ZNodeLabel.Path> {
            ZOOKEEPER_SERVICE;

            public static Splitter PROPERTY_SPLITTER = Splitter.on(PROPERTY_SEPARATOR).omitEmptyStrings();
            public static Splitter KEY_SPLITTER = Splitter.on(KEY_SEPARATOR);
            
            @Override
            public ZNodeLabel.Path apply(ObjectName input) {
                List<ZNodeLabel> components = Lists.newLinkedList();
                components.add(ZNodeLabel.Path.root());
                for (String property: PROPERTY_SPLITTER.split(input.getCanonicalKeyPropertyListString())) {
                    String component = Iterables.toArray(KEY_SPLITTER.split(property), String.class)[1];
                    components.add(ZNodeLabel.Component.of(component));
                }
                return ZNodeLabel.Path.of(components.iterator());
            }
        }
    }

    public static enum Domain implements Function<String, ObjectName> {
        ZOOKEEPER_SERVICE("org.apache.ZooKeeperService"), LOG4J("log4j");
        
        public static char DOMAIN_SEPARATOR = ':';
        public static final Joiner JOINER = Joiner.on(DOMAIN_SEPARATOR);
        
        private final String value;
        
        private Domain(String value) {
            this.value = value;
        }
        
        public String value() {
            return value;
        }

        @Override
        public ObjectName apply(String input) {
            String[] parts = {value(), input};
            try {
                return ObjectName.getInstance(JOINER.join(parts));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
    
    public static enum Key {
        STANDALONE_SERVER("StandaloneServer_port%d"),
        REPLICATED_SERVER("ReplicatedServer_id%d"),
        REPLICA("replica.%d"),
        LEADER("Leader"), 
        FOLLOWER("Follower"),
        LEADER_ELECTION("LeaderElection"),
        IN_MEMORY_DATA_TREE("InMemoryDataTree");
        
        private final String value;
        
        private Key(String value) {
            this.value = value;
        }
        
        public String value() {
            return value;
        }
    }
    

    public static class ObjectNameNode extends ZNodeLabelTrie.AbstractNode<ObjectNameNode> {

        public static ObjectNameNode root() {
            return new ObjectNameNodeFactory().get();
        }

        public static ObjectNameNode childOf(ZNodeLabelTrie.Pointer<ObjectNameNode> parent) {
            return new ObjectNameNodeFactory().get(parent);
        }
        
        public static class ObjectNameNodeFactory implements DefaultsFactory<ZNodeLabelTrie.Pointer<ObjectNameNode>, ObjectNameNode> {

            public ObjectNameNodeFactory() {}
            
            @Override
            public ObjectNameNode get() {
                return new ObjectNameNode(Optional.<ZNodeLabelTrie.Pointer<ObjectNameNode>>absent(), this);
            }

            @Override
            public ObjectNameNode get(ZNodeLabelTrie.Pointer<ObjectNameNode> value) {
                return new ObjectNameNode(Optional.of(value), this);
            }
            
        }
        
        protected final Set<ObjectName> names;

        protected ObjectNameNode(
                Optional<ZNodeLabelTrie.Pointer<ObjectNameNode>> parent,
                DefaultsFactory<ZNodeLabelTrie.Pointer<ObjectNameNode>, ObjectNameNode> factory) {
            super(parent, factory);
            this.names = Sets.newHashSet();
        }

        public Set<ObjectName> names() {
            return names;
        }
        
        @Override 
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("children", children())
                    .add("names", names())
                    .toString();
        }
    }
    
    
    public static enum ServerSchema {
        STANDALONE_SERVER(Key.STANDALONE_SERVER),
        REPLICATED_SERVER(Key.REPLICATED_SERVER);
        
        private final ZNodeLabelTrie<ZNodeLabelTrie.SimpleNode> trie;
        
        private ServerSchema(Key rootKey) {
            this.trie = ZNodeLabelTrie.of(ZNodeLabelTrie.SimpleNode.root());
            ZNodeLabelTrie.SimpleNode root = this.trie.root().put(rootKey.value());
            
            switch (rootKey) {
            case STANDALONE_SERVER:
            {
                root.put(Key.IN_MEMORY_DATA_TREE.value());
                break;
            }
            case REPLICATED_SERVER:
            {
                ZNodeLabelTrie.SimpleNode replica = root.put(Key.REPLICA.value());
                Key[] roles = { Key.FOLLOWER, Key.LEADER, Key.LEADER_ELECTION };
                for (Key k: roles) {
                    ZNodeLabelTrie.SimpleNode role = replica.put(k.value());
                    if (k != Key.LEADER_ELECTION) {
                        role.put(Key.IN_MEMORY_DATA_TREE.value());
                    }
                }
                break;
            }
            default:
                throw new AssertionError();
            }
        }
        
        public ZNodeLabelTrie<ZNodeLabelTrie.SimpleNode> asTrie() {
            return trie;
        }
        
        public ZNodeLabel.Path pathOf(Key key) {
            for (ZNodeLabelTrie.SimpleNode n: asTrie()) {
                ZNodeLabel.Path path = n.path();
                if (path.isRoot()) {
                    continue;
                }
                if (path.tail().toString().equals(key.value())) {
                    return path;
                }
            }
            throw new IllegalArgumentException(key.toString());
        }
        
        public ZNodeLabelTrie<ObjectNameNode> instantiate(MBeanServerConnection mbeans) throws IOException {
            ZNodeLabelTrie<ObjectNameNode> instance = ZNodeLabelTrie.of(ObjectNameNode.root());
            for (ZNodeLabelTrie.SimpleNode n: asTrie()) {
                ZNodeLabel.Path path = n.path();
                if (path.isRoot()) {
                    continue;
                }
                if (path.toString().indexOf('%') >= 0) {
                    // convert format to pattern
                    ObjectName pattern = PathObjectName.of(ZNodeLabel.Path.of(patternOf(path.toString())));
                    Set<ObjectName> results = mbeans.queryNames(pattern, null);
                    if (results.size() > 0) {
                        ObjectNameNode node = instance.put(path);
                        for (ObjectName result: results) {
                            node.names().add(result);
                        }
                    }
                } else {
                    ObjectName result = PathObjectName.of(path);
                    if (mbeans.isRegistered(result)) {
                        ObjectNameNode node = instance.put(path);
                        node.names().add(result);
                    }
                }
            }
            
            return instance;
        }
    }
    
    public static enum PlatformMBeanServerFactory implements Factory<MBeanServer> {
        PLATFORM;
        
        public static PlatformMBeanServerFactory getInstance() {
            return PLATFORM;
        }

        @Override
        public MBeanServer get() {
            return ManagementFactory.getPlatformMBeanServer();
        }
    }

    private Jmx() {}
    
    public static void main(String[] args) throws IOException {
        DefaultsFactory<String, JMXServiceURL> urlFactory = SunAttachQueryJmx.getInstance();
        JMXServiceURL url = (args.length > 0) ? urlFactory.get(args[0]) : urlFactory.get();
        JMXConnector connector = JMXConnectorFactory.connect(url);
        try {
            MBeanServerConnection mbeans = connector.getMBeanServerConnection();
            for (ServerSchema schema: ServerSchema.values()) {
                ZNodeLabelTrie<ObjectNameNode> objectNames = schema.instantiate(mbeans);
                if (! objectNames.isEmpty()) {
                    System.out.println(objectNames.toString());
                }
            }
        } finally {
            connector.close();
        }
    }
}
