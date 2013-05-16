package edu.uw.zookeeper.jmx;

import java.lang.management.ManagementFactory;
import java.util.Hashtable;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.util.Factory;

public abstract class JMX {
    
    public static String patternOf(String format) {
        return format.toString().replaceAll("%[sd]", "*");
    }

    public static enum Domain implements Function<Hashtable<String,String>, ObjectName> {
        ZOOKEEPER_SERVICE("org.apache.ZooKeeperService"), LOG4J("log4j");
        
        public static String DOMAIN_SEPARATOR = ":";
        
        private final String value;
        
        private Domain(String value) {
            this.value = value;
        }
        
        public String value() {
            return value;
        }

        @Override
        public ObjectName apply(Hashtable<String,String> input) {
            try {
                return ObjectName.getInstance(value(), input);
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
        
        public String format() {
            return value;
        }
    }
    
    public static enum ServerFactory implements Factory<MBeanServer> {
        LOCAL;

        @Override
        public MBeanServer get() {
            return ManagementFactory.getPlatformMBeanServer();
        }
    }
    
    public static enum PathToObjectName implements Function<ZNodeLabel.Path, ObjectName> {
        ZOOKEEPER_SERVICE(Domain.ZOOKEEPER_SERVICE);

        public static String KEY_FORMAT = "name%d";
        
        private final Domain domain;
        
        private PathToObjectName(Domain domain) {
            this.domain = domain;
        }
        
        @Override
        public ObjectName apply(ZNodeLabel.Path input) {
            Hashtable<String,String> properties = new Hashtable<String,String>();
            int index = 0;
            for (ZNodeLabel.Component component: input) {
                properties.put(String.format(KEY_FORMAT, index), component.toString());
                index += 1;
            }
            return domain.apply(properties);
        }
    }
}
