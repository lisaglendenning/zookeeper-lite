package edu.uw.zookeeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;

public abstract class Cfg {
    
    public static Properties load(String path) throws IOException {
        Properties cfg = new Properties();
        load(cfg, path);
        String dynamicPath = Key.DYNAMIC_CONFIG_FILE.getValue(cfg);
        if (dynamicPath != null) {
            Properties dynamicCfg = new Properties(cfg);
            return load(dynamicCfg, dynamicPath);
        } else {
            return cfg;
        }
    }

    public static Properties load(Properties properties, String path) throws IOException {
        FileInputStream file = new FileInputStream(new File(path));
        try {
            properties.load(file);
            return properties;
        } finally {
            file.close();
        }
    }
    
    public enum Key {
        CLIENT_PORT,
        CLIENT_PORT_ADDRESS,
        DYNAMIC_CONFIG_FILE;
        
        public String key() {
            return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name());
        }
        
        public String getValue(Properties properties) {
            return properties.getProperty(key());
        }
        
        public boolean hasValue(Properties properties) {
            return getValue(properties) != null;
        }
    }
    
    public static final class Server {
        public static final String SERVER = "server";
        public static enum Group {
            ID {
                @Override
                public String expr() {
                    return "\\d+";
                }
            };
            
            public abstract String expr();
            
            public String key() {
                return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name());
            }
            
            public String value(Matcher m) {
                return m.group(key());
            }
            
            @Override
            public String toString() {
                return "(?<" + key() +">" + expr() + ")";
            }
        }
        
        public static final Pattern PATTERN = Pattern.compile(SERVER + "\\." + Group.ID.toString());

        public static final String FORMAT = SERVER + ".%d";
        
        public static Matcher matcher(String input) {
            return PATTERN.matcher(input);
        }

        public static Server valueOf(String input) {
            Matcher m = matcher(input);
            if (!m.matches()) {
                throw new IllegalArgumentException(input);
            }
            return valueOf(Integer.parseInt(Group.ID.value(m)));
        }
        
        public static Server valueOf(int id) {
            return new Server(id);
        }
        
        private final int id;
        
        protected Server(int id) {
            this.id = id;
        }
        
        public int getId() {
            return id;
        }
        
        @Override
        public String toString() {
            return String.format(FORMAT, id);
        }
        
        @Override
        public int hashCode() {
            return id;
        }
        
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (! (obj instanceof Server)) {
                return false;
            }
            Server other = (Server) obj;
            return id == other.id;
        }
    }

    // note that these patterns are underspecified
    public static final String PORT_RE = "\\d+";
    public static final String ADDRESS_RE = "[^:]+";
    
    // < 3.5.0
    public static final class StaticServer {
        public static enum Group {
            HOSTNAME {
                @Override
                public String expr() {
                    return ADDRESS_RE;
                }
            },
            LEADER_PORT {
                @Override
                public String expr() {
                    return PORT_RE;
                }
            },
            ELECTION_PORT {
                @Override
                public String expr() {
                    return PORT_RE;
                }
            };
            
            public abstract String expr();
            
            public String key() {
                return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name());
            }
            
            public String value(Matcher m) {
                return m.group(key());
            }
            
            @Override
            public String toString() {
                return "(?<" + key() +">" + expr() + ")";
            }
        }
        public static final String DEFAULT_HOSTNAME = "";
        public static final int DEFAULT_ELECTION_PORT = -1;
        
        public static final Pattern PATTERN = Pattern.compile(
                "(?:" + Group.HOSTNAME.toString() + ")?"
                + ":"+ Group.LEADER_PORT.toString()
                + "(?::" + Group.ELECTION_PORT.toString() + ")?");
        
        public static final String FORMAT = "%s:%d:%d";
        public static final String FORMAT_NO_HOSTNAME = ":%d:%d";
        public static final String FORMAT_NO_ELECTION_PORT = "%s:%d";
        public static final String FORMAT_MIN = ":%d";
        
        public static Matcher matcher(String input) {
            return PATTERN.matcher(input);
        }

        public static StaticServer valueOf(String input) {
            Matcher m = matcher(input);
            if (!m.matches()) {
                throw new IllegalArgumentException(input);
            }
            return valueOf(m);
        }
        
        public static StaticServer valueOf(Matcher m) {
            String hostname = Group.HOSTNAME.value(m);
            if (hostname == null) {
                hostname = DEFAULT_HOSTNAME;
            }
            int leaderPort = Integer.parseInt(Group.LEADER_PORT.value(m));
            String electionPort = Group.ELECTION_PORT.value(m);
            if (electionPort == null) {
                return valueOf(hostname, leaderPort);
            } else {
                return valueOf(hostname, leaderPort, Integer.parseInt(electionPort));
            }
        }
        
        public static StaticServer valueOf(String hostname,
                int leaderPort,
                int electionPort) {
            return new StaticServer(
                    hostname, 
                    leaderPort, 
                    electionPort);
        }

        public static StaticServer valueOf(String hostname,
                int leaderPort) {
            return valueOf(
                    hostname, 
                    leaderPort, 
                    DEFAULT_ELECTION_PORT);
        }

        public static StaticServer valueOf(
                int leaderPort,
                int electionPort) {
            return valueOf(
                    DEFAULT_HOSTNAME, 
                    leaderPort, 
                    electionPort);
        }

        public static StaticServer valueOf(
                int leaderPort) {
            return valueOf(
                    DEFAULT_HOSTNAME, 
                    leaderPort, 
                    DEFAULT_ELECTION_PORT);
        }
        
        public static String toString(String hostname,
                int leaderPort,
                int electionPort) {
            return String.format(FORMAT,
                    hostname, 
                    leaderPort, 
                    electionPort);
        }

        public static String toString(String hostname,
                int leaderPort) {
            return String.format(FORMAT_NO_ELECTION_PORT,
                    hostname, 
                    leaderPort);
        }

        public static String toString(
                int leaderPort,
                int electionPort) {
            return String.format(FORMAT_NO_HOSTNAME,
                    leaderPort, 
                    electionPort);
        }

        public static String toString(
                int leaderPort) {
            return String.format(FORMAT_MIN,
                    leaderPort);
        }
        
        private final String hostname;
        private final int leaderPort;
        private final int electionPort;
        
        protected StaticServer(String hostname,
                int leaderPort,
                int electionPort) {
            this.hostname = hostname;
            this.leaderPort = leaderPort;
            this.electionPort = electionPort;
        }
        
        public String getHostname() {
            return hostname;
        }
        
        public int getLeaderPort() {
            return leaderPort;
        }
        
        public int getElectionPort() {
            return electionPort;
        }
        
        @Override
        public String toString() {
            if (hostname.equals(DEFAULT_HOSTNAME)) {
                if (electionPort == DEFAULT_ELECTION_PORT) {
                    return toString(
                            leaderPort);
                } else {
                    return toString(
                            leaderPort,
                            electionPort);
                }
            } else {
                if (electionPort == DEFAULT_ELECTION_PORT) {
                    return toString(
                            hostname,
                            leaderPort);
                } else {
                    return toString(
                            hostname, 
                            leaderPort,
                            electionPort);    
                }
            }
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(hostname);
        }
        
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (! (obj instanceof StaticServer)) {
                return false;
            }
            StaticServer other = (StaticServer) obj;
            return Objects.equal(hostname, other.hostname)
                    && (leaderPort == other.leaderPort)
                    && (electionPort == other.electionPort);
        }
    }
    
    // >= 3.5.0
    public static final class DynamicServer {
        public static enum Role {
            PARTICIPANT, OBSERVER;
            
            public static Role fromString(String input) {
                for (Role v: values()) {
                    if (input.equals(v.toString())) {
                        return v;
                    }
                }
                throw new IllegalArgumentException(input);
            }
            
            @Override
            public String toString() {
                return name().toLowerCase();
            }
        }
        
        public static enum Group {
            ADDRESS {
                @Override
                public String expr() {
                    return ADDRESS_RE;
                }
            },
            LEADER_PORT {
                @Override
                public String expr() {
                    return PORT_RE;
                }
            },
            ELECTION_PORT {
                @Override
                public String expr() {
                    return PORT_RE;
                }
            },
            ROLE {
                @Override
                public String expr() {
                    return Joiner.on('|').join(Role.values());
                }
            },
            CLIENT_ADDRESS {
                @Override
                public String expr() {
                    return ADDRESS_RE;
                }
            },
            CLIENT_PORT {
                @Override
                public String expr() {
                    return PORT_RE;
                }
            };
            
            public abstract String expr();
            
            public String key() {
                return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name());
            }
            
            public String value(Matcher m) {
                return m.group(key());
            }
            
            @Override
            public String toString() {
                return "(?<" + key() +">" + expr() + ")";
            }
        }
        
        public static final Role DEFAULT_ROLE = Role.PARTICIPANT;
        public static final String DEFAULT_CLIENT_ADDRESS = "0.0.0.0";
        
        public static final Pattern PATTERN = Pattern.compile(
                Group.ADDRESS.toString() 
                        + ":"+ Group.LEADER_PORT.toString()
                        + ":" + Group.ELECTION_PORT.toString() 
                        + "(?::" + Group.ROLE.toString() + ")?"
                        + ";" + "(?:" + Group.CLIENT_ADDRESS.toString() + ":)?"
                        + Group.CLIENT_PORT.toString());
        
        public static final String FORMAT = "%s:%d:%d:%s;%s:%d";
        public static final String FORMAT_NO_ROLE = "%s:%d:%d;%s:%d";
        public static final String FORMAT_NO_ADDRESS = "%s:%d:%d:%s;%d";
        public static final String FORMAT_MIN = "%s:%d:%d;%d";
        
        public static Matcher matcher(String input) {
            return PATTERN.matcher(input);
        }
        
        public static DynamicServer valueOf(String input) {
            Matcher m = matcher(input);
            if (!m.matches()) {
                throw new IllegalArgumentException(input);
            }
            return valueOf(m);
        }
        
        public static DynamicServer valueOf(Matcher m) {
            String address = Group.ADDRESS.value(m);
            int leaderPort = Integer.parseInt(Group.LEADER_PORT.value(m));
            int electionPort = Integer.parseInt(Group.ELECTION_PORT.value(m));
            String role = Group.ROLE.value(m);
            String clientAddress = Group.CLIENT_ADDRESS.value(m);
            if (clientAddress == null) {
                clientAddress = DEFAULT_CLIENT_ADDRESS;
            }
            int clientPort = Integer.parseInt(Group.CLIENT_PORT.value(m));
            if (role == null) {
                return valueOf(address, leaderPort, electionPort, 
                        clientAddress, clientPort);
            } else {
                return valueOf(address, leaderPort, electionPort, 
                        Role.fromString(role), clientAddress, clientPort);
            }
        }
        
        public static DynamicServer valueOf(String address,
                int leaderPort,
                int electionPort,
                Role role,
                String clientAddress,
                int clientPort) {
            return new DynamicServer(
                    address, 
                    leaderPort, 
                    electionPort, 
                    role, 
                    clientAddress, 
                    clientPort);
        }

        public static DynamicServer valueOf(String address,
                int leaderPort,
                int electionPort,
                String clientAddress,
                int clientPort) {
            return valueOf(
                    address, 
                    leaderPort, 
                    electionPort, 
                    DEFAULT_ROLE, 
                    clientAddress, 
                    clientPort);
        }

        public static DynamicServer valueOf(String address,
                int leaderPort,
                int electionPort,
                Role role,
                int clientPort) {
            return valueOf(
                    address, 
                    leaderPort, 
                    electionPort, 
                    role, 
                    DEFAULT_CLIENT_ADDRESS, 
                    clientPort);
        }

        public static DynamicServer valueOf(String address,
                int leaderPort,
                int electionPort,
                int clientPort) {
            return valueOf(
                    address, 
                    leaderPort, 
                    electionPort, 
                    DEFAULT_ROLE, 
                    DEFAULT_CLIENT_ADDRESS, 
                    clientPort);
        }
        
        public static String toString(String address,
                int leaderPort,
                int electionPort,
                Role role,
                String clientAddress,
                int clientPort) {
            return String.format(FORMAT,
                    address, 
                    leaderPort, 
                    electionPort, 
                    role, 
                    clientAddress, 
                    clientPort);
        }

        public static String toString(String address,
                int leaderPort,
                int electionPort,
                String clientAddress,
                int clientPort) {
            return String.format(FORMAT_NO_ROLE,
                    address, 
                    leaderPort, 
                    electionPort,
                    clientAddress, 
                    clientPort);
        }

        public static String toString(String address,
                int leaderPort,
                int electionPort,
                Role role,
                int clientPort) {
            return String.format(FORMAT_NO_ADDRESS,
                    address, 
                    leaderPort, 
                    electionPort, 
                    role,
                    clientPort);
        }

        public static String toString(String address,
                int leaderPort,
                int electionPort,
                int clientPort) {
            return String.format(FORMAT_MIN,
                    address, 
                    leaderPort, 
                    electionPort,
                    clientPort);
        }
        
        private final String address;
        private final int leaderPort;
        private final int electionPort;
        private final Role role;
        private final String clientAddress;
        private final int clientPort;
        
        protected DynamicServer(String address,
                int leaderPort,
                int electionPort,
                Role role,
                String clientAddress,
                int clientPort) {
            this.address = address;
            this.leaderPort = leaderPort;
            this.electionPort = electionPort;
            this.role = role;
            this.clientAddress = clientAddress;
            this.clientPort = clientPort;
        }
        
        public String getAddress() {
            return address;
        }
        
        public int getLeaderPort() {
            return leaderPort;
        }
        
        public int getElectionPort() {
            return electionPort;
        }
        
        public Role getRole() {
            return role;
        }
        
        public String getClientAddress() {
            return clientAddress;
        }
        
        public int getClientPort() {
            return clientPort;
        }
        
        @Override
        public String toString() {
            if (clientAddress.equals(DEFAULT_CLIENT_ADDRESS)) {
                return toString(
                        address, 
                        leaderPort,
                        electionPort,
                        role,
                        clientPort);
            } else {
                return toString(
                        address, 
                        leaderPort,
                        electionPort,
                        role,
                        clientAddress,
                        clientPort);    
            }
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(address, clientPort);
        }
        
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (! (obj instanceof DynamicServer)) {
                return false;
            }
            DynamicServer other = (DynamicServer) obj;
            return Objects.equal(address, other.address)
                    && (leaderPort == other.leaderPort)
                    && (electionPort == other.electionPort)
                    && Objects.equal(role, other.role)
                    && Objects.equal(clientAddress, other.clientAddress)
                    && (clientPort == other.clientPort);
        }
    }
}
